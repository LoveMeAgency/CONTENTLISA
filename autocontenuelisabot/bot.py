import asyncio
import logging
import os
import shutil
import sqlite3
import ssl
import subprocess
import tempfile
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import certifi
from PIL import Image
from zoneinfo import ZoneInfo

# --- pillow_heif optionnel : le bot démarre même si non installé
try:
    import pillow_heif
    HAVE_HEIF = True
except Exception:
    HAVE_HEIF = False

from pyrogram import Client, filters, idle
from pyrogram.errors import BadRequest, ChatAdminRequired, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

import config

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,  # passe en DEBUG pour diagnostiquer finement
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------- Folders ----------------
BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "session"
SESSION_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Pyrogram Client ----------------
app_1 = Client(
    name=str(SESSION_DIR / "bot1"),
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN_1,
)

# ---------------- SQLite (uniquement pour planifier les suppressions) ----------------
DB_PATH = BASE_DIR / "autopost.sqlite3"

def db_init():
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deletions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                delete_at INTEGER NOT NULL
            )
        """)
        con.commit()

def db_schedule_deletion(chat_id: int, message_id: int, delete_at_ts: int):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO deletions (chat_id, message_id, delete_at) VALUES (?, ?, ?)",
            (chat_id, message_id, delete_at_ts)
        )
        con.commit()

def db_fetch_due_deletions(now_ts: int, limit: int = 200) -> List[Tuple[int, int, int]]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            "SELECT id, chat_id, message_id FROM deletions WHERE delete_at <= ? ORDER BY id ASC LIMIT ?",
            (now_ts, limit)
        )
        return cur.fetchall()

def db_delete_deletion_row(row_id: int):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM deletions WHERE id=?", (row_id,))
        con.commit()

db_init()

# ---------------- Utils ----------------
_FR_WEEKDAYS = {
    "lundi": 0, "mardi": 1, "mercredi": 2, "jeudi": 3,
    "vendredi": 4, "samedi": 5, "dimanche": 6
}

def _kb(buttons: Optional[List[Tuple[str, str]]]) -> Optional[InlineKeyboardMarkup]:
    if not buttons:
        return None
    rows = [[InlineKeyboardButton(text=txt, url=url)] for (txt, url) in buttons]
    return InlineKeyboardMarkup(rows)

def _guess_ext_from_content_type(ct: str, default: str = "") -> str:
    if not ct:
        return default
    m = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/avif": ".avif",
        "image/heic": ".heic",
        "image/heif": ".heif",
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
    }
    return m.get(ct.split(";")[0].strip().lower(), default)

async def _convert_image_if_needed(path: str) -> str:
    """
    Convertit WebP/HEIC/HEIF/AVIF -> JPEG (si supporté) et réduit si dimensions énormes.
    Retourne le chemin final (peut être identique).
    """
    try:
        # active l’opener HEIF seulement si la lib est dispo
        try:
            if HAVE_HEIF:
                pillow_heif.register_heif_opener()
        except Exception:
            pass

        with Image.open(path) as im:
            fmt = (im.format or "").upper()
            problematic = {"WEBP", "HEIC", "HEIF", "AVIF"}
            # si format “compliqué” et qu’on sait le lire -> on convertit
            if fmt in problematic:
                # si fmt est HEIC/HEIF/AVIF et que lib absente, on ne convertit pas (on tente tel quel)
                if fmt in {"HEIC", "HEIF", "AVIF"} and not HAVE_HEIF:
                    return path
                out = path.rsplit(".", 1)[0] + ".jpg"
                rgb = im.convert("RGB")
                max_side = 4096
                w, h = rgb.size
                if max(w, h) > max_side:
                    scale = max_side / float(max(w, h))
                    rgb = rgb.resize((int(w*scale), int(h*scale)))
                rgb.save(out, "JPEG", quality=90, optimize=True)
                return out
            else:
                # JPEG/PNG trop grands ➜ on clamp
                max_side = 4096
                w, h = im.size
                if max(w, h) > max_side:
                    out = path.rsplit(".", 1)[0] + "_tg.jpg"
                    rgb = im.convert("RGB")
                    scale = max_side / float(max(w, h))
                    rgb = rgb.resize((int(w*scale), int(h*scale)))
                    rgb.save(out, "JPEG", quality=90, optimize=True)
                    return out
    except Exception as e:
        logger.warning(f"[image] Conversion ignorée ({e})")
    return path

def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None

async def _convert_video_if_needed(path: str) -> str:
    """
    Normalise en MP4 H.264 + AAC avec moov au début si ffmpeg dispo.
    """
    if not _has_ffmpeg():
        return path
    out = path.rsplit(".", 1)[0] + "_tg.mp4"
    try:
        cmd = [
            "ffmpeg", "-y", "-i", path,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            out
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return out
    except Exception as e:
        logger.warning(f"[video] Conversion ffmpeg échouée ({e})")
        return path

async def _download_if_url(maybe_url: Optional[str]) -> Optional[str]:
    if not maybe_url:
        return None
    s = str(maybe_url)

    # Chemin local
    if not s.startswith(("http://", "https://")):
        return s

    try:
        req = urllib.request.Request(
            s,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
                "Accept": "image/*,video/*;q=0.9,*/*;q=0.8",
                "Referer": "https://my-privatelink.com/",
            }
        )
        ssl_ctx = ssl.create_default_context(cafile=certifi.where()) if s.startswith("https") else None
        with urllib.request.urlopen(req, timeout=120, context=ssl_ctx) as resp:
            ct = resp.headers.get("Content-Type", "")
            suffix = _guess_ext_from_content_type(ct, Path(s).suffix or "")
            fd, temp_path = tempfile.mkstemp(prefix="ap_dl_", suffix=suffix)
            with os.fdopen(fd, "wb") as f:
                f.write(resp.read())

        if os.path.getsize(temp_path) < 1024:
            logger.warning(f"Téléchargement trop petit, probablement HTML/erreur: {s}")
            return None

        return temp_path
    except Exception as e:
        logger.warning(f"Téléchargement media KO {s}: {e}")
        return None

def _seconds_until_next_weekly(weekday_idx: int, hour: int, minute: int, tz_str: str) -> float:
    tz = ZoneInfo(tz_str)
    now = datetime.now(tz)
    days_ahead = (weekday_idx - now.weekday()) % 7
    target = (now + timedelta(days=days_ahead)).replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=7)
    return (target - now).total_seconds()

def _resolve_schedule_tuple(schedule_var_name: str) -> Tuple[int, int, int]:
    """Lit POSTX_SCHEDULE dans config et renvoie (weekday_idx, hour, minute)."""
    if not hasattr(config, schedule_var_name):
        raise ValueError(f"Variable horaire manquante dans config.py: {schedule_var_name}")
    day_str, hhmm = getattr(config, schedule_var_name)
    day_idx = _FR_WEEKDAYS.get(day_str.strip().lower())
    if day_idx is None:
        raise ValueError(f"Jour invalide '{day_str}' pour {schedule_var_name}")
    hour, minute = map(int, hhmm.strip().split(":"))
    return day_idx, hour, minute

async def _resolve_chat_id(chat_ref: int | str) -> Optional[int]:
    """
    Accepte un int (-100...) ou un @username.
    Retourne l'ID numérique (-100...) ou None en cas d'échec.
    """
    try:
        if isinstance(chat_ref, str) and not chat_ref.lstrip("-").isdigit():
            chat = await app_1.get_chat(chat_ref)  # ex: "@mychannel"
            return chat.id
        return int(chat_ref)
    except Exception as e:
        logger.warning(f"[resolve] Impossible de résoudre {chat_ref}: {e}")
        return None

# ---------------- Messages (TOUT est ici) ----------------
MESSAGES: List[Dict[str, Any]] = [
    # ... (la même liste MESSAGES que celle que tu utilises déjà)
    # Je laisse ton contenu tel quel pour ne rien casser.
    # ------------------ DEBUT COPIE TON LISTING ------------------
    {
        "name": "post1",
        "schedule_var": "POST1_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Coucou mes bébés 🫦",
        "buttons": [],
    },
    {
        "name": "post2",
        "schedule_var": "POST2_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je penses fort à vous aujourd'hui 🫣",
        "buttons": [],
    },
    {
        "name": "post3",
        "schedule_var": "POST3_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Et d’ailleurs je vous ai laissé une surprise ici:\n\n👉 **[ACCÉDER AU CONTENU 🔞](https://my-privatelink.com/lisaa)**\n\nTu viens ?",
        "buttons": [(":🔞 ACCÉDER AU CONTENU (1.14€) 🔞", "https://my-privatelink.com/lisaa/")],
    },
    {
        "name": "post4",
        "schedule_var": "POST4_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/IMG_7602.jpg",
        "text": "Ne me fait pas attendre...",
        "buttons": [],
    },
    {
        "name": "post5",
        "schedule_var": "POST5_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je prépare du nouveau contenu rien que pour vous 🍑\n**Rejoins ici si tu veux accéder:\n\n👉 https://my-privatelink.com/lisaa**\n\n__(c'est le truc à 1.14€)__",
        "buttons": [(":🔞 ACCÉDER AU CONTENU 🔞", "https://my-privatelink.com/lisaa/")],
    },
    {
        "name": "post6",
        "schedule_var": "POST6_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Un petit avant goût...😇",
        "buttons": [],
    },
    {
        "name": "post7",
        "schedule_var": "POST7_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Si tu veux tout voir (TOUT)\n\nÇa se passe ici:\n👉 **[DÉBLOQUER TOUT MON CONTENU 🔞](https://my-privatelink.com/lisaa)**",
        "buttons": [],
    },
    {
        "name": "post8",
        "schedule_var": "POST8_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je suis en manque...",
        "buttons": [],
    },
    {
        "name": "post9",
        "schedule_var": "POST9_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "C’est trop grave si je passe plus de 10h sans rien je me sens mal 😿",
        "buttons": [],
    },
    {
        "name": "post10",
        "schedule_var": "POST10_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je vais devoir régler ça 🫣",
        "buttons": [],
    },
    {
        "name": "post11",
        "schedule_var": "POST11_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je lance un Live pour qu’on s’amuse ensemble ??? 🌶️💕",
        "buttons": [],
    },
    {
        "name": "post12",
        "schedule_var": "POST12_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Allez c'est parti:\n👉 **[ACCÉDER AU LIVE 🫦](https://my-privatelink.com/lisaa)**\n👉 **[ACCÉDER AU LIVE 🫦](https://my-privatelink.com/lisaa)**\n👉 **[ACCÉDER AU LIVE 🫦](https://my-privatelink.com/lisaa)**",
        "buttons": [(":🔞 ACCÉDER AU LIVE 🔞", "https://my-privatelink.com/lisaa/")],
    },
    {
        "name": "post13",
        "schedule_var": "POST13_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je t'attends",
        "buttons": [],
    },
    {
        "name": "post14",
        "schedule_var": "POST14_SCHEDULE",
        "type": "video",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/video-output-F29DD972-C7A2-4F3B-A0BF-C5DAC18F5374-1.mp4",
        "text": None,
        "buttons": [(":🔞 ACCÉDER AU LIVE 🔞", "https://my-privatelink.com/lisaa/")],
    },
    {
        "name": "post15",
        "schedule_var": "POST15_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Dépêche toi de venir 🙈🙈",
        "buttons": [],
    },
    {
        "name": "post16",
        "schedule_var": "POST16_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je vais bientôt jouir 💦",
        "buttons": [],
    },
    {
        "name": "post17",
        "schedule_var": "POST17_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Hello mes chéris vous allez bien ?? 💕💕",
        "buttons": [],
    },
    {
        "name": "post18",
        "schedule_var": "POST18_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je viens de sortir de la douche..🧼",
        "buttons": [],
    },
    {
        "name": "post19",
        "schedule_var": "POST19_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Une petite exclusivité rien que pour toi 🫣\n👇\nhttps://my-privatelink.com/lisaa",
        "buttons": [],
    },
    {
        "name": "post20",
        "schedule_var": "POST20_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/photo-output.jpeg",
        "text": None,
        "buttons": [],
    },
    {
        "name": "post21",
        "schedule_var": "POST21_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Bon allez je vais vous poster ce que je viens de filmer 💦👅",
        "buttons": [],
    },
    {
        "name": "post22",
        "schedule_var": "POST22_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/photo-output-2.jpeg",
        "text": None,
        "buttons": [],
    },
    {
        "name": "post23",
        "schedule_var": "POST23_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Le nouveau contenue est dispo sur mon espace privé ✨\n1️⃣ Clique ici: https://my-privatelink.com/lisaa\n2️⃣ Inscris-toi\n3️⃣ Prends la période d’essai à 1.14€\n4️⃣ Accède à tout mon contenu (à vie)😘",
        "buttons": [(":🔞 ACCÉDER A MON CONTENU 🔞", "https://my-privatelink.com/lisaa/")],
    },
    {
        "name": "post24",
        "schedule_var": "POST24_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Bon je vais retourner me 🤟🏼👀",
        "buttons": [],
    },
    {
        "name": "post25",
        "schedule_var": "POST25_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "C’est trop bon 🤤",
        "buttons": [],
    },
    {
        "name": "post26",
        "schedule_var": "POST26_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Mais je préférerai avoir ta bite à la place de mes doigts 🫦",
        "buttons": [],
    },
    {
        "name": "post27",
        "schedule_var": "POST27_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je vais chez un mec tout à l’heure 🫣",
        "buttons": [],
    },
    {
        "name": "post28",
        "schedule_var": "POST28_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je vous partagerai tout ça si vous voulez 🤭",
        "buttons": [],
    },
    {
        "name": "post29",
        "schedule_var": "POST29_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je suis chez lui on va pas tarder a commencer 🫦",
        "buttons": [],
    },
    {
        "name": "post30",
        "schedule_var": "POST30_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je lui ai dit de filmer vous allez adorer 🥰",
        "buttons": [],
    },
    {
        "name": "post31",
        "schedule_var": "POST31_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Tiens la vidéo🤭\n👉**[ACCÉDER AU VIP 🫦](https://my-privatelink.com/lisaa)**\n👉**[ACCÉDER AU VIP 🫦](https://my-privatelink.com/lisaa)**\n👉**[ACCÉDER AU VIP 🫦](https://my-privatelink.com/lisaa)**\n**Et le petit cadeau: accès à vie pour seulement 1.14€**",
        "buttons": [],
    },
    {
        "name": "post32",
        "schedule_var": "POST32_SCHEDULE",
        "type": "video",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/video-output-70E14AA7-59E2-426F-9DEB-586A3292BDF1-1.mp4",
        "text": None,
        "buttons": [],
    },
    {
        "name": "post33",
        "schedule_var": "POST33_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je suis dans mon bain tu viens ??? 🫣",
        "buttons": [],
    },
    {
        "name": "post34",
        "schedule_var": "POST34_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/photo-output-6.jpeg",
        "text": None,
        "buttons": [(":🔞 VOIR MON CONTENU", "https://my-privatelink.com/lisaa/")],
    },
    {
        "name": "post35",
        "schedule_var": "POST35_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Vous aimez bien 🤪",
        "buttons": [],
    },
    {
        "name": "post36",
        "schedule_var": "POST36_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Coucouuuu 🤓",
        "buttons": [],
    },
    {
        "name": "post37",
        "schedule_var": "POST37_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Qui veut venir s’occuper de moi 🍑",
        "buttons": [],
    },
    {
        "name": "post38",
        "schedule_var": "POST38_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Je vais devoir m’amuser toute seule ? 🤟🏼",
        "buttons": [],
    },
    {
        "name": "post39",
        "schedule_var": "POST39_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Oh je viens de retrouver mon petit jouet 🍆",
        "buttons": [],
    },
    {
        "name": "post40",
        "schedule_var": "POST40_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Vous voulez que je vous montre ce que ça donne 👀",
        "buttons": [],
    },
    {
        "name": "post41",
        "schedule_var": "POST41_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/photo-output-4.jpeg",
        "text": None,
        "buttons": [],
    },
    {
        "name": "post42",
        "schedule_var": "POST42_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "👉 **[ACCÉDER AU VIP 🫦](https://my-privatelink.com/lisaa)**\n👉**[ACCÉDER AU VIP 🫦](https://my-privatelink.com/lisaa)**\n👉 **[ACCÉDER AU VIP 🫦](https://my-privatelink.com/lisaa)**\n\nEt le petit cadeau: acces à vie pour 1.14€ 💝💝",
        "buttons": [],
    },
    {
        "name": "post43",
        "schedule_var": "POST43_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Heyyyyy 💕💕💕💕💕💕",
        "buttons": [],
    },
    {
        "name": "post44",
        "schedule_var": "POST44_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "J’ai mis ma tenue du dimanche",
        "buttons": [],
    },
    {
        "name": "post45",
        "schedule_var": "POST45_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/photo-output-3.jpeg",
        "text": None,
        "buttons": [],
    },
    {
        "name": "post46",
        "schedule_var": "POST46_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Vous aimez bien ???",
        "buttons": [],
    },
    {
        "name": "post47",
        "schedule_var": "POST47_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Ou vous préférez sans 🤓",
        "buttons": [],
    },
    {
        "name": "post48",
        "schedule_var": "POST48_SCHEDULE",
        "type": "photo",
        "media": "http://my-privatelink.com/wp-content/uploads/2025/12/photo-output-5.jpeg",
        "text": None,
        "buttons": [],
    },
    {
        "name": "post49",
        "schedule_var": "POST49_SCHEDULE",
        "type": "text",
        "media": None,
        "text": "Allez viens voir mon contenu :\n👉 https://my-privatelink.com/lisaa\n__C'est à 1.14€ à vie__",
        "buttons": [],
    },

    # ------------------ FIN COPIE TON LISTING ------------------
]

# ---------------- Envoi d’un post vers 1 canal ----------------
async def _send_autopost_to_chat(chat_ref: int | str, post_cfg: Dict[str, Any]) -> Optional[int]:
    """
    Envoie un post vers chat_ref (int -100... ou @username).
    Résout d'abord l'ID numérique. Télécharge/convertit les médias si besoin.
    """
    ptype = (post_cfg.get("type") or "text").lower()
    media = post_cfg.get("media")
    text = post_cfg.get("text") or ""
    buttons = post_cfg.get("buttons")
    markup = _kb(buttons)

    chat_id = await _resolve_chat_id(chat_ref)
    if chat_id is None:
        logger.warning(f"[autopost] Résolution chat KO pour {chat_ref}")
        return None

    temp_path = None
    conv_path = None
    try:
        media_path = None
        if ptype in ("photo", "video", "voice", "document"):
            media_path = await _download_if_url(media)
            if not media_path:
                logger.warning(f"[autopost] Média introuvable pour {post_cfg.get('name')}")
                return None
            if os.path.isabs(media_path):
                temp_path = media_path

            if ptype == "photo":
                conv_path = await _convert_image_if_needed(media_path)
                media_path = conv_path or media_path
            elif ptype == "video":
                conv_path = await _convert_video_if_needed(media_path)
                media_path = conv_path or media_path

        if ptype == "text":
            m = await app_1.send_message(chat_id, text or " ", reply_markup=markup)
        elif ptype == "photo":
            m = await app_1.send_photo(chat_id, photo=media_path, caption=text or None, reply_markup=markup)
        elif ptype == "video":
            m = await app_1.send_video(chat_id, video=media_path, caption=text or None, reply_markup=markup, supports_streaming=True)
        elif ptype == "voice":
            m = await app_1.send_voice(chat_id, voice=media_path, caption=text or None, reply_markup=markup)
        elif ptype == "document":
            m = await app_1.send_document(chat_id, document=media_path, caption=text or None, reply_markup=markup)
        else:
            m = await app_1.send_message(chat_id, text or " ", reply_markup=markup)

        return m.id
    except ChatAdminRequired:
        logger.warning(f"[autopost] Pas les droits dans {chat_id} (publier/supprimer).")
    except BadRequest as e:
        logger.warning(f"[autopost] BadRequest {chat_id}: {e}")
    except RPCError as e:
        logger.warning(f"[autopost] RPCError {chat_id}: {e}")
    except Exception as e:
        logger.warning(f"[autopost] Unexpected {chat_id}: {e}")
    finally:
        for p in (conv_path, temp_path):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
    return None

# ---------------- Workers ----------------
async def _autopost_worker(post_cfg: Dict[str, Any]):
    """Planifie et envoie ce post chaque semaine au jour/heure donnés, dans tous les CHANNEL_IDS."""
    tz = ZoneInfo(config.TIMEZONE)
    wd, h, m = _resolve_schedule_tuple(post_cfg["schedule_var"])

    while True:
        wait_s = _seconds_until_next_weekly(wd, h, m, config.TIMEZONE)
        logger.info(f"[autopost] {post_cfg['name']} prochain envoi dans {int(wait_s)}s ({post_cfg['schedule_var']}).")
        await asyncio.sleep(max(1, wait_s))

        if not getattr(config, "CHANNEL_IDS", None):
            logger.info("[autopost] Aucun CHANNEL_IDS dans config.py — envoi ignoré.")
        else:
            sent = 0
            for raw_ref in config.CHANNEL_IDS:  # int -100... ou "@username"
                mid = await _send_autopost_to_chat(raw_ref, post_cfg)
                if mid:
                    delete_at = int((datetime.now(tz) + timedelta(days=config.AUTO_DELETE_AFTER_DAYS)).timestamp())
                    chat_id = await _resolve_chat_id(raw_ref)
                    if chat_id is not None:
                        db_schedule_deletion(chat_id, mid, delete_at)
                    sent += 1
                await asyncio.sleep(0.25)
            logger.info(f"[autopost] {post_cfg['name']} envoyé dans {sent} canal(aux).")

        # recalcul pour itération suivante
        wd, h, m = _resolve_schedule_tuple(post_cfg["schedule_var"])

async def _autodelete_worker():
    """Supprime périodiquement les messages arrivés à échéance (toutes les ~10 min)."""
    while True:
        now_ts = int(datetime.now().timestamp())
        rows = db_fetch_due_deletions(now_ts, limit=200)
        if rows:
            logger.info(f"[autodelete] À supprimer: {len(rows)} messages")
        for row_id, chat_id, message_id in rows:
            try:
                await app_1.delete_messages(chat_id, message_id)
            except Exception as e:
                logger.warning(f"[autodelete] {chat_id}:{message_id} -> {e}")
            finally:
                db_delete_deletion_row(row_id)
            await asyncio.sleep(0.2)
        await asyncio.sleep(600)

# ---------------- Commandes admin (test & debug) ----------------
@app_1.on_message(filters.command("force_post_index") & filters.user(config.ADMIN_ID))
async def force_post_index_handler(client: Client, message: Message):
    parts = message.text.strip().split()
    if len(parts) != 2:
        return await message.reply_text("Usage: /force_post_index <index 0-based>")
    try:
        idx = int(parts[1])
        post = MESSAGES[idx]
    except Exception:
        return await message.reply_text("Index invalide.")
    if not getattr(config, "CHANNEL_IDS", None):
        return await message.reply_text("Aucun CHANNEL_IDS dans config.py.")
    tz = ZoneInfo(config.TIMEZONE)
    sent = 0
    for raw_ref in config.CHANNEL_IDS:
        mid = await _send_autopost_to_chat(raw_ref, post)
        if mid:
            delete_at = int((datetime.now(tz) + timedelta(days=config.AUTO_DELETE_AFTER_DAYS)).timestamp())
            chat_id = await _resolve_chat_id(raw_ref)
            if chat_id is not None:
                db_schedule_deletion(chat_id, mid, delete_at)
            sent += 1
        await asyncio.sleep(0.25)
    await message.reply_text(f"OK: post {idx} envoyé dans {sent} canal(aux).")

@app_1.on_message(filters.command("start") & filters.user(config.ADMIN_ID))
async def start_handler(client: Client, message: Message):
    await message.reply_text("Bot OK. Utilise /force_post_index <i> pour tester un envoi.")

@app_1.on_message(filters.command("resolve") & filters.user(config.ADMIN_ID))
async def resolve_handler(client: Client, message: Message):
    # /resolve @username_ou_-100id
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply_text("Usage: /resolve <@username ou -100id>")
    raw = parts[1]
    try:
        chat = await app_1.get_chat(raw)
        await message.reply_text(f"OK ✅\nTitle: {chat.title}\nType: {chat.type}\nID: {chat.id}")
    except Exception as e:
        await message.reply_text(f"KO ❌: {e}")

# ---------------- Préflight (sanity check droits & accès) ----------------
async def _preflight_check():
    try:
        me = await app_1.get_me()
        for raw in getattr(config, "CHANNEL_IDS", []):
            try:
                chat = await app_1.get_chat(raw)
                # Tentative de lecture des privilèges si le bot est admin
                try:
                    member = await app_1.get_chat_member(chat.id, me.id)
                    can_post = getattr(getattr(member, "privileges", None), "can_post_messages", None)
                    can_delete = getattr(getattr(member, "privileges", None), "can_delete_messages", None)
                    logger.info(f"[preflight] {chat.title} ({chat.id}) -> can_post={can_post} can_delete={can_delete}")
                except Exception as e:
                    logger.warning(f"[preflight] Impossible de lire les droits sur {chat.id}: {e}")
            except Exception as e:
                logger.warning(f"[preflight] Accès impossible à {raw}: {e}")
    except Exception as e:
        logger.warning(f"[preflight] Erreur globale: {e}")

# ---------------- Main (Pyrogram v2) ----------------
async def main():
    await app_1.start()

    # Préflight immédiat
    await _preflight_check()

    # Lancer un worker par post
    for post_cfg in MESSAGES:
        asyncio.create_task(_autopost_worker(post_cfg))

    # Lancer le worker de suppression
    asyncio.create_task(_autodelete_worker())

    # Log de sanity check statique
    try:
        for p in MESSAGES:
            day, hm = getattr(config, p["schedule_var"])
            logger.info(f"[startup] {p['name']} -> {day} {hm}")
        logger.info(f"[startup] CHANNEL_IDS = {getattr(config, 'CHANNEL_IDS', [])}")
    except Exception:
        pass

    await idle()
    await app_1.stop()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass

import html
import io
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response, FileResponse
from PIL import Image, ImageDraw, ImageFont

AUTHOR_LOGIN = os.getenv("AUTHOR_LOGIN", "1154202")
IMAGE_DIR = Path(os.getenv("IMAGE_DIR", "/tmp/lab12_images"))
IMAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Lab 12. Yandex Serverless Applications")

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
MAX_IMAGE_SIZE = 2000
MIN_IMAGE_SIZE = 10


def page(title: str, body: str) -> str:
    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{html.escape(title)}</title>
        <style>
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: #f3f4f6;
                color: #111827;
            }}
            header {{
                background: #111827;
                color: white;
                padding: 18px 28px;
            }}
            main {{
                max-width: 1000px;
                margin: 28px auto;
                padding: 0 20px;
            }}
            .card {{
                background: white;
                border-radius: 16px;
                padding: 24px;
                box-shadow: 0 10px 25px rgba(0,0,0,.08);
                margin-bottom: 18px;
            }}
            label {{ display: block; margin-top: 14px; font-weight: 700; }}
            input {{
                width: 100%;
                box-sizing: border-box;
                padding: 11px 12px;
                margin-top: 6px;
                border: 1px solid #d1d5db;
                border-radius: 10px;
                font-size: 16px;
            }}
            button, .button {{
                display: inline-block;
                margin-top: 18px;
                padding: 11px 18px;
                border: none;
                border-radius: 10px;
                background: #2563eb;
                color: white;
                text-decoration: none;
                cursor: pointer;
                font-size: 16px;
            }}
            nav a {{ color: white; margin-right: 18px; }}
            .error {{ color: #dc2626; font-weight: 700; }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
                gap: 18px;
            }}
            .tile {{
                background: white;
                border-radius: 16px;
                padding: 12px;
                box-shadow: 0 8px 20px rgba(0,0,0,.08);
            }}
            .tile img {{
                width: 100%;
                height: 170px;
                object-fit: cover;
                border-radius: 12px;
                background: #e5e7eb;
            }}
            .muted {{ color: #6b7280; }}
        </style>
    </head>
    <body>
        <header>
            <h1>Лабораторная работа №12</h1>
            <nav>
                <a href="/makeimage">Создать изображение</a>
                <a href="/load_image">Загрузить изображение</a>
                <a href="/images">Галерея</a>
                <a href="/login">/login</a>
            </nav>
        </header>
        <main>{body}</main>
    </body>
    </html>
    """


def makeimage_form(message: str = "") -> str:
    error_block = f'<p class="error">message: "{html.escape(message)}"</p>' if message else ""
    return page(
        "Создание изображения",
        f"""
        <section class="card">
            <h2>Создание JPG-изображения</h2>
            <p class="muted">Введите ширину, высоту и текст. После отправки сервер вернёт готовый JPG-файл.</p>
            {error_block}
            <form method="POST" action="/makeimage" enctype="application/x-www-form-urlencoded">
                <label for="width">width</label>
                <input id="width" name="width" type="number" min="{MIN_IMAGE_SIZE}" max="{MAX_IMAGE_SIZE}" value="600" required>

                <label for="height">height</label>
                <input id="height" name="height" type="number" min="{MIN_IMAGE_SIZE}" max="{MAX_IMAGE_SIZE}" value="400" required>

                <label for="text">text</label>
                <input id="text" name="text" type="text" value="Hello, Serverless!">

                <button type="submit">Создать изображение</button>
            </form>
        </section>
        """,
    )


def upload_form(message: str = "") -> str:
    error_block = f'<p class="error">{html.escape(message)}</p>' if message else ""
    return page(
        "Загрузка изображения",
        f"""
        <section class="card">
            <h2>Загрузка изображения</h2>
            <p class="muted">Если поле названия оставить пустым, оно автоматически заполнится именем выбранного файла.</p>
            {error_block}
            <form method="POST" action="/load_image" enctype="multipart/form-data">
                <label for="image_name">Название изображения</label>
                <input id="image_name" name="image_name" type="text" placeholder="Например: my_photo">

                <label for="image_file">Файл изображения</label>
                <input id="image_file" name="image_file" type="file" accept="image/png,image/jpeg,image/webp" required>

                <button type="submit">Загрузить</button>
            </form>
        </section>
        <script>
            const nameInput = document.getElementById('image_name');
            const fileInput = document.getElementById('image_file');
            fileInput.addEventListener('change', () => {{
                if (!nameInput.value && fileInput.files.length > 0) {{
                    const rawName = fileInput.files[0].name;
                    nameInput.value = rawName.replace(/[.][^/.]+$/, '');
                }}
            }});
        </script>
        """,
    )


def parse_image_size(value: str) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if MIN_IMAGE_SIZE <= number <= MAX_IMAGE_SIZE:
        return number
    return None


def get_font(width: int, height: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    font_size = max(14, min(width, height) // 10)
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/local/share/fonts/DejaVuSans.ttf",
        "DejaVuSans.ttf",
    ]
    for font_path in candidates:
        try:
            return ImageFont.truetype(font_path, font_size)
        except OSError:
            continue
    return ImageFont.load_default()


def create_jpeg(width: int, height: int, text: str) -> bytes:
    image = Image.new("RGB", (width, height), color=(230, 230, 230))
    draw = ImageDraw.Draw(image)
    font = get_font(width, height)
    safe_text = text if text else " "
    bbox = draw.textbbox((0, 0), safe_text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    x = max((width - text_width) // 2, 0)
    y = max((height - text_height) // 2, 0)
    draw.text((x, y), safe_text, fill=(20, 20, 20), font=font)

    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


def slugify_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^\w\-.а-яА-ЯёЁ]", "", name, flags=re.UNICODE)
    return name or f"image_{uuid.uuid4().hex[:8]}"


def unique_created_filename() -> str:
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"created_{now}_{uuid.uuid4().hex[:6]}.jpg"


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(
        page(
            "Главная",
            """
            <section class="card">
                <h2>Yandex Serverless Applications</h2>
                <p>Приложение поддерживает маршруты /login, /makeimage, /load_image и /images.</p>
                <a class="button" href="/makeimage">Перейти к созданию изображения</a>
            </section>
            """,
        )
    )


@app.get("/login")
async def login() -> JSONResponse:
    return JSONResponse({"author": AUTHOR_LOGIN})


@app.get("/makeimage", response_class=HTMLResponse)
async def makeimage_get() -> HTMLResponse:
    return HTMLResponse(makeimage_form())


@app.post("/makeimage")
async def makeimage_post(
    width: str = Form(...),
    height: str = Form(...),
    text: str = Form(""),
) -> Response:
    image_width = parse_image_size(width)
    image_height = parse_image_size(height)
    if image_width is None or image_height is None:
        return HTMLResponse(makeimage_form("Invalid image size"), status_code=400)

    jpeg_bytes = create_jpeg(image_width, image_height, text)
    filename = unique_created_filename()
    (IMAGE_DIR / filename).write_bytes(jpeg_bytes)

    return Response(content=jpeg_bytes, media_type="image/jpeg")


@app.get("/load_image", response_class=HTMLResponse)
async def load_image_get() -> HTMLResponse:
    return HTMLResponse(upload_form())


@app.post("/load_image", response_class=HTMLResponse)
async def load_image_post(
    image_name: str = Form(""),
    image_file: UploadFile = File(...),
) -> HTMLResponse:
    original_filename = image_file.filename or "uploaded_image"
    extension = Path(original_filename).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        return HTMLResponse(upload_form("Ошибка: можно загружать только JPG, PNG или WEBP."), status_code=400)

    base_name = image_name.strip() or Path(original_filename).stem
    safe_name = slugify_name(base_name)
    final_filename = f"{safe_name}{extension}"
    final_path = IMAGE_DIR / final_filename

    if final_path.exists():
        return HTMLResponse(upload_form("Ошибка: указанное имя изображения уже занято."), status_code=400)

    content = await image_file.read()
    try:
        with Image.open(io.BytesIO(content)) as img:
            img.verify()
    except Exception:
        return HTMLResponse(upload_form("Ошибка: файл не является корректным изображением."), status_code=400)

    final_path.write_bytes(content)
    return HTMLResponse(
        page(
            "Изображение загружено",
            f"""
            <section class="card">
                <h2>Изображение успешно загружено</h2>
                <p>Файл сохранён как <b>{html.escape(final_filename)}</b>.</p>
                <a class="button" href="/images">Открыть галерею</a>
            </section>
            """,
        )
    )


@app.get("/images", response_class=HTMLResponse)
async def images() -> HTMLResponse:
    files = sorted(
        [p for p in IMAGE_DIR.iterdir() if p.is_file() and p.suffix.lower() in ALLOWED_EXTENSIONS],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not files:
        content = """
        <section class="card">
            <h2>Галерея изображений</h2>
            <p class="muted">Пока нет созданных или загруженных изображений.</p>
        </section>
        """
    else:
        tiles = []
        for file_path in files:
            quoted = quote(file_path.name)
            tiles.append(
                f"""
                <div class="tile">
                    <img src="/image/{quoted}" alt="{html.escape(file_path.name)}">
                    <p><b>{html.escape(file_path.name)}</b></p>
                </div>
                """
            )
        content = f"""
        <section class="card">
            <h2>Галерея изображений</h2>
            <p class="muted">Ниже отображаются все ранее созданные и загруженные изображения.</p>
        </section>
        <section class="grid">{''.join(tiles)}</section>
        """

    return HTMLResponse(page("Галерея", content))


@app.get("/image/{filename}")
async def get_image(filename: str):
    safe_filename = Path(filename).name
    file_path = IMAGE_DIR / safe_filename
    if not file_path.exists() or not file_path.is_file():
        return JSONResponse({"result": "image not found"}, status_code=404)

    content_type = "image/jpeg"
    if file_path.suffix.lower() == ".png":
        content_type = "image/png"
    elif file_path.suffix.lower() == ".webp":
        content_type = "image/webp"
    return FileResponse(file_path, media_type=content_type)

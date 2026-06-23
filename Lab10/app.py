from io import BytesIO
import textwrap
from flask import Flask, Response, jsonify, request, render_template_string
from PIL import Image, ImageDraw, ImageFont

app = Flask(__name__)

AUTHOR_LOGIN = "1154202"
MIN_SIZE = 10
MAX_SIZE = 2000

HTML_TEMPLATE = """
<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Генерация JPG изображения</title>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            font-family: Arial, sans-serif;
            background: #f3f4f6;
            color: #111827;
        }
        .card {
            width: min(560px, calc(100% - 32px));
            background: white;
            padding: 28px;
            border-radius: 18px;
            box-shadow: 0 18px 45px rgba(15, 23, 42, 0.12);
        }
        h1 { margin: 0 0 10px; font-size: 28px; }
        p { margin: 0 0 22px; color: #4b5563; }
        label { display: block; margin: 14px 0 6px; font-weight: 700; }
        input {
            width: 100%;
            padding: 12px 14px;
            border: 1px solid #d1d5db;
            border-radius: 10px;
            font-size: 16px;
        }
        button {
            width: 100%;
            margin-top: 22px;
            border: 0;
            border-radius: 12px;
            padding: 13px 16px;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
            background: #111827;
            color: white;
        }
        .message {
            margin: 0 0 16px;
            padding: 12px 14px;
            border-radius: 10px;
            background: #fee2e2;
            color: #b91c1c;
            font-weight: 700;
        }
        .hint { margin-top: 16px; font-size: 14px; color: #6b7280; }
        .preview-box { margin-top: 22px; display: none; }
        .preview-box img { width: 100px; height: 100px; border-radius: 10px; border: 1px solid #d1d5db; object-fit: cover; }
    </style>
</head>
<body>
    <main class="card">
        <h1>Генератор изображения</h1>
        <p>Введите размер и текст. Сервер вернёт готовый JPG-файл.</p>

        {% if message %}
            <div class="message">message: "{{ message }}"</div>
        {% endif %}

        <form method="POST" action="/makeimage" enctype="application/x-www-form-urlencoded">
            <label for="width">Width, px</label>
            <input id="width" name="width" type="number" min="10" max="2000" value="{{ width or 600 }}" required>

            <label for="height">Height, px</label>
            <input id="height" name="height" type="number" min="10" max="2000" value="{{ height or 300 }}" required>

            <label for="text">Text</label>
            <input id="text" name="text" type="text" value="{{ text or 'Hello from Flask' }}" required>

            <button type="submit">Создать JPG</button>
        </form>

        <div class="preview-box" id="previewBox">
            <div class="hint">Предпросмотр 100×100:</div>
            <img id="previewImage" alt="thumbnail preview">
        </div>

        <div class="hint">Допустимый размер: от 10 до 2000 пикселей по каждой стороне.</div>
    </main>

    <script>
        const widthInput = document.querySelector('#width');
        const heightInput = document.querySelector('#height');
        const textInput = document.querySelector('#text');
        const previewBox = document.querySelector('#previewBox');
        const previewImage = document.querySelector('#previewImage');
        let timer = null;
        let previousUrl = null;

        function updatePreview() {
            clearTimeout(timer);
            timer = setTimeout(async () => {
                const data = new URLSearchParams();
                data.append('width', widthInput.value);
                data.append('height', heightInput.value);
                data.append('text', textInput.value);

                try {
                    const response = await fetch('/thumbnail', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                        body: data.toString()
                    });
                    if (!response.ok) {
                        previewBox.style.display = 'none';
                        return;
                    }
                    const blob = await response.blob();
                    if (previousUrl) URL.revokeObjectURL(previousUrl);
                    previousUrl = URL.createObjectURL(blob);
                    previewImage.src = previousUrl;
                    previewBox.style.display = 'block';
                } catch (error) {
                    previewBox.style.display = 'none';
                }
            }, 400);
        }

        widthInput.addEventListener('input', updatePreview);
        heightInput.addEventListener('input', updatePreview);
        textInput.addEventListener('input', updatePreview);
        updatePreview();
    </script>
</body>
</html>
"""


def validate_size(width_raw: str, height_raw: str) -> tuple[int | None, int | None, bool]:
    try:
        width = int(width_raw)
        height = int(height_raw)
    except (TypeError, ValueError):
        return None, None, False

    valid = MIN_SIZE <= width <= MAX_SIZE and MIN_SIZE <= height <= MAX_SIZE
    return width, height, valid


def generate_image(width: int, height: int, text: str) -> bytes:
    image = Image.new("RGB", (width, height), color=(235, 238, 242))
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()

    safe_text = text.strip() or "No text"
    max_symbols_per_line = max(8, width // 10)
    lines = textwrap.wrap(safe_text, width=max_symbols_per_line) or [safe_text]
    lines = lines[:12]

    line_heights = []
    line_widths = []
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        line_widths.append(bbox[2] - bbox[0])
        line_heights.append(bbox[3] - bbox[1])

    spacing = 6
    total_height = sum(line_heights) + spacing * (len(lines) - 1)
    y = max(0, (height - total_height) // 2)

    for line, line_width, line_height in zip(lines, line_widths, line_heights):
        x = max(0, (width - line_width) // 2)
        draw.text((x, y), line, fill=(17, 24, 39), font=font)
        y += line_height + spacing

    output = BytesIO()
    image.save(output, format="JPEG", quality=92)
    return output.getvalue()


@app.get("/login")
def login() -> Response:
    return jsonify({"author": AUTHOR_LOGIN})


@app.route("/makeimage", methods=["GET", "POST"])
def makeimage() -> Response | str:
    if request.method == "GET":
        return render_template_string(HTML_TEMPLATE, message=None, width=None, height=None, text=None)

    width_raw = request.form.get("width")
    height_raw = request.form.get("height")
    text = request.form.get("text", "")
    width, height, valid = validate_size(width_raw, height_raw)

    if not valid:
        return render_template_string(
            HTML_TEMPLATE,
            message="Invalid image size",
            width=width_raw,
            height=height_raw,
            text=text,
        ), 400

    jpg_data = generate_image(width, height, text)
    return Response(jpg_data, mimetype="image/jpeg")


@app.post("/thumbnail")
def thumbnail() -> Response:
    width_raw = request.form.get("width")
    height_raw = request.form.get("height")
    text = request.form.get("text", "")
    _, _, valid = validate_size(width_raw, height_raw)

    if not valid:
        return Response(status=400)

    jpg_data = generate_image(100, 100, text)
    return Response(jpg_data, mimetype="image/jpeg")


@app.get("/health")
def health() -> Response:
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)

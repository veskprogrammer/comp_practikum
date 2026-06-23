# Лабораторная работа №12

## Тема

**Облачные приложения — Yandex Serverless Applications**

## Цель работы

Разработать веб-приложение на Python с использованием библиотеки Pillow, реализовать генерацию изображений по запросу пользователя и подготовить приложение к развёртыванию в **Yandex Serverless Containers**.

## Используемые технологии

- Python 3.12;
- FastAPI;
- Uvicorn;
- Pillow;
- Docker;
- Yandex Serverless Containers.

## Описание задания

В рамках лабораторной работы было необходимо создать веб-приложение, которое поддерживает следующие маршруты:

- `/login` — возвращает логин автора в формате JSON;
- `/makeimage` — отображает HTML-форму и генерирует JPG-изображение по параметрам формы;
- `/load_image` — позволяет загрузить изображение через HTML-форму;
- `/images` — отображает плиткой все ранее созданные и загруженные изображения.

## Структура проекта

```text
lab12_yandex_serverless_app/
├── main.py
├── requirements.txt
├── Dockerfile
├── .dockerignore
├── .gitignore
└── README.md
```

## Реализованные маршруты

### Маршрут `/login`

Метод: `GET`

Маршрут возвращает JSON-ответ с логином автора:

```json
{"author": "1154202"}
```

### Маршрут `/makeimage`

Поддерживает методы `GET` и `POST`.

Метод `GET` возвращает HTML-страницу с формой. В форме есть поля:

- `width` — ширина изображения;
- `height` — высота изображения;
- `text` — текст, который будет нарисован на изображении.

Метод `POST` принимает данные из формы, проверяет размеры изображения и при успешной валидации создаёт JPG-файл.

Если ширина или высота указаны неверно, сервер возвращает ту же страницу с сообщением:

```text
message: "Invalid image size"
```

При успешной генерации изображение возвращается сразу в браузер с заголовком:

```text
Content-Type: image/jpeg
```

Также созданное изображение сохраняется во временную папку приложения и отображается в галерее `/images`.

### Маршрут `/load_image`

Поддерживает методы `GET` и `POST`.

Метод `GET` возвращает страницу с формой загрузки изображения. В форме есть:

- поле названия изображения;
- поле выбора файла;
- кнопка загрузки.

Если поле названия оставить пустым, оно автоматически заполняется именем выбранного файла с помощью JavaScript.

Метод `POST` принимает изображение и сохраняет его. Если файл с таким названием уже существует, загрузка отменяется и выводится сообщение об ошибке.

### Маршрут `/images`

Метод: `GET`

Маршрут выводит плиткой все ранее созданные и загруженные изображения. Для каждого изображения отображается миниатюра и имя файла.

## Основной код приложения

```python
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response, FileResponse
from PIL import Image, ImageDraw, ImageFont
```

Приложение создаётся с помощью FastAPI:

```python
app = FastAPI(title="Lab 12. Yandex Serverless Applications")
```

Маршрут `/login`:

```python
@app.get("/login")
async def login() -> JSONResponse:
    return JSONResponse({"author": AUTHOR_LOGIN})
```

Генерация изображения выполняется с помощью Pillow:

```python
def create_jpeg(width: int, height: int, text: str) -> bytes:
    image = Image.new("RGB", (width, height), color=(230, 230, 230))
    draw = ImageDraw.Draw(image)
    font = get_font(width, height)
    bbox = draw.textbbox((0, 0), text, font=font)
    x = max((width - (bbox[2] - bbox[0])) // 2, 0)
    y = max((height - (bbox[3] - bbox[1])) // 2, 0)
    draw.text((x, y), text, fill=(20, 20, 20), font=font)

    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()
```

## Dockerfile

Для развёртывания в Yandex Serverless Containers был подготовлен `Dockerfile`:

```dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    AUTHOR_LOGIN=1154202

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
```

Контейнер запускает приложение на порту `8080`, который подходит для запуска в serverless-контейнере.

## Запуск проекта локально

Сначала нужно установить зависимости:

```bash
pip install -r requirements.txt
```

Затем запустить приложение:

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8080
```

После запуска приложение будет доступно по адресу:

```text
http://localhost:8080
```

Проверка маршрута `/login`:

```bash
curl http://localhost:8080/login
```

Ожидаемый ответ:

```json
{"author":"1154202"}
```

## Запуск через Docker

Сборка образа:

```bash
docker build -t lab12-serverless-app .
```

Запуск контейнера:

```bash
docker run --rm -p 8080:8080 lab12-serverless-app
```

После запуска можно открыть:

```text
http://localhost:8080/makeimage
http://localhost:8080/load_image
http://localhost:8080/images
```

## Развёртывание в Yandex Serverless Containers

Общий порядок развёртывания:

1. Создать Docker-образ приложения.
2. Загрузить образ в Container Registry.
3. Создать Serverless Container в Yandex Cloud.
4. Указать загруженный Docker-образ.
5. Настроить порт `8080`.
6. Открыть публичный URL контейнера.
7. Проверить маршруты `/login`, `/makeimage`, `/load_image`, `/images`.

## Вывод

В ходе выполнения лабораторной работы было разработано веб-приложение на Python с использованием FastAPI и Pillow. Приложение поддерживает маршруты, указанные в задании: `/login`, `/makeimage`, `/load_image` и `/images`.

Была реализована генерация JPG-изображения по параметрам пользователя, проверка корректности размеров изображения, загрузка пользовательских изображений и отображение всех изображений в виде плиточной галереи. Для подготовки приложения к облачному запуску был создан `Dockerfile`, позволяющий собрать контейнер и развернуть его в Yandex Serverless Containers.

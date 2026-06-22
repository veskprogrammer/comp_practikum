# size2json-app

Веб-приложение для задания по маршрутам `/login` и `/size2json`.

## Корневой адрес приложения

После деплоя сюда нужно вставить HTTPS-адрес:

```text
https://your-app-name.onrender.com
```

## Репозиторий с кодом

После публикации на GitHub сюда нужно вставить ссылку:

```text
https://github.com/your-login/size2json-app
```

## Маршруты

### GET `/login`

Возвращает логин Moodle в JSON:

```json
{"author":"1154202"}
```

### POST `/size2json`

Принимает PNG-файл по имени поля `image` в формате `multipart/form-data`.

Пример успешного ответа:

```json
{"width":123,"height":456}
```

Если передан не PNG-файл:

```json
{"result":"invalid filetype"}
```

## Установка и запуск локально

```bash
npm install
npm start
```

После запуска приложение доступно по адресу:

```text
http://localhost:3000
```

## Проверка через curl

Проверка логина:

```bash
curl http://localhost:3000/login
```

Проверка PNG:

```bash
curl -X POST http://localhost:3000/size2json -F "image=@test.png"
```

Проверка неправильного файла:

```bash
curl -X POST http://localhost:3000/size2json -F "image=@test.txt"
```

## Реализованные усложнения

- Есть фронтенд-страница с формой загрузки файла.
- Файл отправляется асинхронно через `fetch`, без перезагрузки страницы.
- Последний загруженный файл сохраняется в `localStorage`.
- Загруженное изображение отображается на странице как thumbnail.
- Все ответы сервера возвращаются в формате JSON с корректным `Content-Type: application/json`.

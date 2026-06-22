# ЛР-5. Веб-приложение для GET- и POST-запросов

## Описание

Веб-приложение реализовано на Node.js с использованием фреймворка Express.

Приложение обрабатывает:

- GET-запросы;
- POST-запросы с JSON-данными;
- возвращает ответы в формате JSON.

## Маршруты

### GET /

Проверка работы приложения.

### GET /hello?name=Георгий

Возвращает приветствие с именем, переданным в query-параметре.

### POST /echo

Возвращает JSON-данные, отправленные в теле запроса.

Пример тела запроса:

```json
{
  "text": "test",
  "author": "Георгий"
}
```

### POST /sum

Складывает два числа.

Пример тела запроса:

```json
{
  "a": 10,
  "b": 15
}
```

## Запуск

```bash
npm install
npm start
```

После запуска приложение будет доступно по адресу:

```text
http://localhost:3000
```

## Проверка через cURL

GET-запрос:

```bash
curl http://localhost:3000/hello?name=Georgiy
```

POST-запрос `/echo`:

```bash
curl -X POST http://localhost:3000/echo -H "Content-Type: application/json" -d "{"text":"test","author":"Georgiy"}"
```

POST-запрос `/sum`:

```bash
curl -X POST http://localhost:3000/sum -H "Content-Type: application/json" -d "{"a":10,"b":15}"
```

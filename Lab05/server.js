const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware для чтения JSON из тела POST-запросов
app.use(express.json());
app.use(cors());

// Корневой GET-запрос
app.get('/', (req, res) => {
  res.json({
    title: 'ЛР-5. GET и POST запросы',
    message: 'Веб-приложение работает',
    routes: {
      getHello: 'GET /hello?name=Георгий',
      postEcho: 'POST /echo',
      postSum: 'POST /sum'
    }
  });
});

// GET-запрос с query-параметром
app.get('/hello', (req, res) => {
  const name = req.query.name || 'гость';

  res.json({
    method: 'GET',
    message: `Привет, ${name}!`,
    query: req.query
  });
});

// POST-запрос, который возвращает отправленные данные
app.post('/echo', (req, res) => {
  res.json({
    method: 'POST',
    message: 'Данные успешно получены',
    received: req.body
  });
});

// POST-запрос для сложения двух чисел
app.post('/sum', (req, res) => {
  const { a, b } = req.body;

  if (typeof a !== 'number' || typeof b !== 'number') {
    return res.status(400).json({
      error: 'Поля a и b должны быть числами'
    });
  }

  res.json({
    method: 'POST',
    a,
    b,
    result: a + b
  });
});

app.listen(PORT, () => {
  console.log(`Server is running: http://localhost:${PORT}`);
});

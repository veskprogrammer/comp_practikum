const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { imageSize } = require('image-size');

const app = express();
const PORT = process.env.PORT || 3000;
const AUTHOR_LOGIN = process.env.AUTHOR_LOGIN || '1154202';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024
  }
});

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

app.get('/login', (req, res) => {
  res.type('application/json');
  res.json({ author: AUTHOR_LOGIN });
});

app.post('/size2json', upload.single('image'), (req, res) => {
  res.type('application/json');

  if (!req.file || req.file.mimetype !== 'image/png') {
    return res.status(400).json({ result: 'invalid filetype' });
  }

  try {
    const size = imageSize(req.file.buffer);

    if (size.type !== 'png' || !size.width || !size.height) {
      return res.status(400).json({ result: 'invalid filetype' });
    }

    return res.json({
      width: size.width,
      height: size.height
    });
  } catch (error) {
    return res.status(400).json({ result: 'invalid filetype' });
  }
});

app.use((req, res) => {
  res.status(404).type('application/json').json({ result: 'not found' });
});

app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}`);
});

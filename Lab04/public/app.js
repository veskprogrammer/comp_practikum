const form = document.querySelector('#uploadForm');
const imageInput = document.querySelector('#imageInput');
const resultBlock = document.querySelector('#result');
const previewBox = document.querySelector('#previewBox');
const preview = document.querySelector('#preview');
const lastFile = document.querySelector('#lastFile');

const saved = localStorage.getItem('lastUpload');
if (saved) {
  lastFile.textContent = saved;
}

imageInput.addEventListener('change', () => {
  const file = imageInput.files[0];
  if (!file) return;

  if (file.type === 'image/png') {
    preview.src = URL.createObjectURL(file);
    previewBox.classList.remove('hidden');
  } else {
    previewBox.classList.add('hidden');
  }
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();

  const file = imageInput.files[0];
  if (!file) {
    resultBlock.textContent = JSON.stringify({ result: 'invalid filetype' }, null, 2);
    return;
  }

  const formData = new FormData();
  formData.append('image', file);

  try {
    const response = await fetch('/size2json', {
      method: 'POST',
      body: formData
    });

    const data = await response.json();
    resultBlock.textContent = JSON.stringify(data, null, 2);

    const lastUploadText = `${file.name} — ${new Date().toLocaleString()}`;
    localStorage.setItem('lastUpload', lastUploadText);
    lastFile.textContent = lastUploadText;
  } catch (error) {
    resultBlock.textContent = JSON.stringify({ result: 'request error' }, null, 2);
  }
});

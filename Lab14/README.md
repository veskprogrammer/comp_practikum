# Лабораторная работа 14 — скрапинг преподавателей РГПУ им. А. И. Герцена

## Что сделано

Реализованы два варианта скрапинга:

1. `herzen_bs4_scraper.py` — через `requests` и `BeautifulSoup`.
2. `herzen_lxml_scraper.py` — через `requests` и `lxml`.

Оба варианта проходят страницы:

```text
https://atlas.herzen.spb.ru/teachers?page=1
...
https://atlas.herzen.spb.ru/teachers?page=54
```

Затем каждый скрипт заходит в профиль преподавателя и извлекает контакты: ФИО, почту и телефон при наличии.

Итоговый CSV содержит колонки:

```text
ФИО, Почта, Телефон, Ссылка на профиль
```

Ссылка на профиль добавлена дополнительно, чтобы можно было проверить источник данных.

## Установка зависимостей

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Для macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск варианта BeautifulSoup

```bash
python herzen_bs4_scraper.py
```

После выполнения появится файл:

```text
teachers_contacts_bs4.csv
```

## Запуск варианта lxml

```bash
python herzen_lxml_scraper.py
```

После выполнения появится файл:

```text
teachers_contacts_lxml.csv
```

## Настройки запуска

Можно изменить страницы, задержку и имя файла:

```bash
python herzen_bs4_scraper.py --start-page 1 --end-page 54 --delay 0.2 --output teachers_contacts_bs4.csv
python herzen_lxml_scraper.py --start-page 1 --end-page 54 --delay 0.2 --output teachers_contacts_lxml.csv
```

Параметр `--delay` нужен, чтобы не отправлять слишком много запросов подряд.

## Файлы в проекте

- `Лабораторная_работа_14_готово.ipynb` — notebook с двумя решениями и пояснениями.
- `herzen_bs4_scraper.py` — первый способ, BeautifulSoup.
- `herzen_lxml_scraper.py` — второй способ, lxml.
- `requirements.txt` — зависимости.
- `teachers_seed_pages_1_54.csv` — промежуточный список преподавателей из приложенного архива: ФИО, ссылка, id, страница. Он оставлен как пример структуры промежуточного результата.

## Примечание

В CSV телефоны и почты объединяются через `;`, если на странице профиля найдено несколько контактов.

"""
Скрапинг преподавателей РГПУ им. А. И. Герцена.
Вариант 2: requests + lxml.

Результат: CSV с колонками ФИО, Почта, Телефон, Ссылка на профиль.
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin

import requests
from lxml import html as lxml_html
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://atlas.herzen.spb.ru"
LIST_URL = BASE_URL + "/teachers?page={page}"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; educational-scraper/1.0; +https://atlas.herzen.spb.ru)",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-zА-Яа-я]{2,}")
PHONE_RE = re.compile(
    r"(?:(?:\+7|8)\s*[\(\- ]?\s*\d{3}\s*[\)\- ]?\s*\d{3}\s*[\- ]?\s*\d{2}\s*[\- ]?\s*\d{2}"
    r"(?:\s*(?:доб\.?|добавочный|ext\.?)\s*\d{1,6})?)"
)
TEACHER_URL_RE = re.compile(r"/teachers/(\d+)(?:$|[?#])")


@dataclass(frozen=True)
class Teacher:
    fio: str
    profile_url: str
    page: int | None = None


@dataclass(frozen=True)
class TeacherContacts:
    fio: str
    email: str
    phone: str
    profile_url: str


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_html(session: requests.Session, url: str, timeout: int = 25) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    if not response.encoding:
        response.encoding = response.apparent_encoding
    return response.text


def unique_keep_order(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = value.strip(" ,;\n\t")
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_teachers_from_list_page(html: str, page: int) -> list[Teacher]:
    """Парсит одну страницу списка через XPath."""
    doc = lxml_html.fromstring(html)
    teachers: list[Teacher] = []
    seen_urls: set[str] = set()

    links = doc.xpath('//a[contains(@href, "/teachers/")]')
    for link in links:
        href = link.get("href")
        fio = normalize_spaces(" ".join(link.xpath(".//text()")))
        if not href or not fio:
            continue

        profile_url = urljoin(BASE_URL, href)
        if not TEACHER_URL_RE.search(profile_url):
            continue
        if profile_url in seen_urls:
            continue
        if len(fio.split()) < 2:
            continue

        teachers.append(Teacher(fio=fio, profile_url=profile_url, page=page))
        seen_urls.add(profile_url)

    return teachers


def collect_teachers(session: requests.Session, start_page: int = 1, end_page: int = 54, delay: float = 0.2) -> list[Teacher]:
    all_teachers: list[Teacher] = []
    seen_urls: set[str] = set()

    for page in range(start_page, end_page + 1):
        url = LIST_URL.format(page=page)
        page_teachers = parse_teachers_from_list_page(get_html(session, url), page)

        added = 0
        for teacher in page_teachers:
            if teacher.profile_url not in seen_urls:
                all_teachers.append(teacher)
                seen_urls.add(teacher.profile_url)
                added += 1

        print(f"Страница {page}: найдено {len(page_teachers)}, добавлено {added}, всего {len(all_teachers)}")
        time.sleep(delay + random.random() * delay)

    return all_teachers


def parse_contacts_from_profile(html: str) -> tuple[str, str]:
    """Достаёт почты и телефоны со страницы профиля через текст документа."""
    doc = lxml_html.fromstring(html)

    # Удаляем script/style, чтобы они не попадали в общий текст.
    for bad_node in doc.xpath("//script|//style|//noscript"):
        parent = bad_node.getparent()
        if parent is not None:
            parent.remove(bad_node)

    text = " ".join(part.strip() for part in doc.xpath("//body//text()") if part.strip())
    emails = unique_keep_order(EMAIL_RE.findall(text))
    phones = unique_keep_order(PHONE_RE.findall(text))
    return "; ".join(emails), "; ".join(phones)


def collect_contacts(session: requests.Session, teachers: list[Teacher], delay: float = 0.2) -> list[TeacherContacts]:
    rows: list[TeacherContacts] = []
    total = len(teachers)

    for index, teacher in enumerate(teachers, start=1):
        try:
            email, phone = parse_contacts_from_profile(get_html(session, teacher.profile_url))
        except Exception as exc:  # noqa: BLE001
            print(f"Ошибка профиля {teacher.profile_url}: {exc}")
            email, phone = "", ""

        rows.append(TeacherContacts(teacher.fio, email, phone, teacher.profile_url))

        if index % 25 == 0 or index == total:
            print(f"Профили: {index}/{total}")
        time.sleep(delay + random.random() * delay)

    return rows


def save_csv(rows: list[TeacherContacts], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["ФИО", "Почта", "Телефон", "Ссылка на профиль"],
            delimiter=",",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "ФИО": row.fio,
                    "Почта": row.email,
                    "Телефон": row.phone,
                    "Ссылка на профиль": row.profile_url,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Скрапинг преподавателей РГПУ им. А. И. Герцена через lxml")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--end-page", type=int, default=54)
    parser.add_argument("--delay", type=float, default=0.2, help="Базовая задержка между запросами в секундах")
    parser.add_argument("--output", default="teachers_contacts_lxml.csv")
    args = parser.parse_args()

    session = make_session()
    teachers = collect_teachers(session, args.start_page, args.end_page, args.delay)
    contacts = collect_contacts(session, teachers, args.delay)
    save_csv(contacts, args.output)

    with_email = sum(bool(row.email) for row in contacts)
    with_phone = sum(bool(row.phone) for row in contacts)
    print(f"Готово: {args.output}")
    print(f"Всего строк: {len(contacts)}; с почтой: {with_email}; с телефоном: {with_phone}")


if __name__ == "__main__":
    main()

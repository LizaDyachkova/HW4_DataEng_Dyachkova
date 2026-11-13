# Правила отбора из HW3, commit 07c8223da8701afddd007bfd863558c521b4261b
# S3-функции удалены: в ДЗ4 источник — локальный исходный CSV.
"""Общие функции для ДЗ 3: подключение к S3, чтение объекта кусками, правило отбора вакансий.

Совместимо с Python 3.9.
"""
import csv
import re


csv.field_size_limit(2 ** 31 - 1)

# Правило отбора вакансий варианта «Программист, разработчик на C# и .NET»
# 1) role = «Программист, разработчик»
# 2) в названии вакансии (name) ИЛИ в ключевых навыках (key_skills) встречается
#    C#, C Sharp, .NET или dotnet (регистр не важен).
# Поле description НЕ используется: там слишком много случайных упоминаний.
ROLE = "Программист, разработчик"

# Тот же шаблон написан в skills.sql (DuckDB, движок RE2) — они должны совпадать.
VARIANT_PATTERN = r"c#|c ?sharp|\.net([^a-z]|$)|dotnet"
VARIANT_RE = re.compile(VARIANT_PATTERN)

# В данных встречается русская буква «с» вместо латинской «c» в «С#».
# Перед поиском и подсчётом заменяем «с#» (кириллица U+0441) на «c#» (латиница).
CYR_CSHARP = "с#"
LAT_CSHARP = "c#"

# Быстрая предпроверка по сырым байтам строки CSV (оптимизация). Это надмножество
# правила: любое совпадение с VARIANT_RE содержит один из этих кусков.
PREFILTER_TOKENS = (b"#", b".net", b"sharp", b"dotnet")


def norm_text(text):
    return text.lower().replace(CYR_CSHARP, LAT_CSHARP)


def norm_skill(text):
    return text.strip(" ").lower().replace(CYR_CSHARP, LAT_CSHARP)


def matches_variant(name, key_skills):
    return bool(VARIANT_RE.search(norm_text(name)) or VARIANT_RE.search(norm_text(key_skills)))


def parse_skills(key_skills):
    """Уникальные навыки вакансии (нормализованные), отсортированные по алфавиту."""
    seen = set()
    for part in key_skills.split(";"):
        skill = norm_skill(part)
        if skill:
            seen.add(skill)
    return sorted(seen)


def prefilter(line_bytes):
    low = line_bytes.lower()
    for token in PREFILTER_TOKENS:
        if token in low:
            return True
    return False



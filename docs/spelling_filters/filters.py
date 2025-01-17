#
# Copyright 2024 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import re

from enchant.tokenize import Filter, tokenize, unit_tokenize
from sphinx.util import logging

logger = logging.getLogger(__name__)


class list_tokenize(tokenize):
    def __init__(self, words):
        super().__init__("")
        self._words = words

    def next(self):
        if not self._words:
            raise StopIteration()
        word = self._words.pop(0)
        return (word, 0)


class CamelCaseFilter(Filter):
    """Split camelcase words and verify they've been spelled correctly."""

    camel_case_pattern = re.compile(r"^[A-Z|a-z][a-z]+[A-Z]+")

    not_camel_case = ["SaaS", "SHapley", "exPlanations"]

    def _check_camel_case(self, word):
        return bool(re.match(self.camel_case_pattern, word))

    def _split_camel_case(self, word):
        return re.findall(r"[a-zA-Z][^A-Z]+", word)

    def _split(self, word):
        # Fixed responses
        if self._check_camel_case(word) and word not in self.not_camel_case:
            return list_tokenize(self._split_camel_case(word))
        return unit_tokenize(word)


class ObjectIdFilter(Filter):
    """Identify and ignore object ids."""

    object_id_regex = re.compile(r"^[0-9a-fA-F]{24}$")

    def _skip(self, word):
        return bool(re.match(self.object_id_regex, word))


class DocstringFilePathFilter(Filter):
    """Split classname words and verify they've been spelled correctly."""

    # These are words that might appear in file paths, but we can safely ignore
    words_to_ignore = ["datarobot"]

    def _split(self, word: str):
        # Fixed responses
        delimiter = ""
        if "/" in word:
            delimiter = "/"
        elif "." in word:
            delimiter = "."
        if delimiter:
            wordlist = [
                word
                for word in word.strip("<").strip(">").split(delimiter)
                if word not in self.words_to_ignore
            ]
            return list_tokenize(wordlist)
        return unit_tokenize(word)


class OrdinalFilter(Filter):
    """Identify and ignore ordinal numbers."""

    ordinal_pattern = re.compile(r"^[0-9]+(st|nd|rd|th)")

    def _check_ordinal(self, word):
        return bool(re.match(self.ordinal_pattern, word))

    def _skip(self, word):
        return self._check_ordinal(word)


class DayMonthFilter(Filter):
    day_month_abbreviations = [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
    ]
    day_of_week = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]

    def _skip(self, word):
        lower_word = word.lower()
        return (
            lower_word in self.day_month_abbreviations
            or lower_word in self.day_of_week
            or lower_word in self.months
        )


class TypeAnnotationFilter(Filter):
    type_annotation_regex = re.compile(r"^([^\[]+)\[([^\[\]]+)]?")

    def _split(self, word):
        match = re.match(self.type_annotation_regex, word)
        if match:
            return list_tokenize([match.group(1), match.group(2)])
        return unit_tokenize(word)


class SortOrderFilter(Filter):
    sort_order_regex = re.compile(r"^sort='-?([a-z]+)")

    def _split(self, word):
        match = re.match(self.sort_order_regex, word)
        if match:
            return unit_tokenize(match.group(1))
        return unit_tokenize(word)


class DataRobotParameterFilter(Filter):
    """For filtering out items like datarobot-key or datarobot_key"""

    parameter_tokens = ["-", "_"]

    def _skip(self, word):
        return "datarobot" in word and any(token in word for token in self.parameter_tokens)

import json
from decimal import Decimal
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

class Exporter:
    @staticmethod
    def json(rows, cols, file_path) -> None:
        result = [dict(zip(cols, row)) for row in rows]

        def convert(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError

        data = json.dumps(result, ensure_ascii=False, indent=2, default=convert)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

    @staticmethod
    def xml(rows, columns, file_path) -> None:
        root = Element("data")
        for row in rows:
            item = SubElement(root, "row")
            for k, v in zip(columns, row):
                SubElement(item, k).text = str(v)

        raw_str = tostring(root, encoding="utf-8")
        parsed = minidom.parseString(raw_str)
        data = parsed.toprettyxml(indent="  ")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

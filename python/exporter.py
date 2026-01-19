import json
from decimal import Decimal
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom


class Exporter:
    """
    Exports query results to JSON or XML files.
    """

    @staticmethod
    def json(rows, cols, file_path) -> None:
        """
        Export query results to a JSON file.

        :param rows: List of database rows
        :param cols: List of column names
        :param file_path: Path to the output JSON file
        """
        result = [dict(zip(cols, row)) for row in rows]

        def convert(obj):
            """
            Convert non-serializable objects to JSON-compatible types.
            """
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError

        data = json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=convert
        )

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

    @staticmethod
    def xml(rows, columns, file_path) -> None:
        """
        Export query results to an XML file.

        :param rows: List of database rows
        :param columns: List of column names
        :param file_path: Path to the output XML file
        """
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

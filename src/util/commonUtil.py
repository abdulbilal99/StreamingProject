from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json

class commonUtil:
    @staticmethod
    def xmlschemaGenerator(json_config):
        def create_struct(fields_config):
            return StructType([create_field(field) for field in fields_config])

        def create_field(field_config):
            field_name = field_config.get('name', '')
            field_type = field_config.get('type', '')
            nullable = field_config.get('nullable', True)

            if field_type == 'string':
                return StructField(field_name, StringType(), nullable=nullable)
            elif field_type == 'integer':
                return StructField(field_name, IntegerType(), nullable=nullable)
            elif field_type == 'struct':
                nested_fields = field_config.get('fields', [])
                return StructField(field_name, create_struct(nested_fields), nullable=nullable)
            else:
                raise ValueError(f"Unsupported data type: {field_type}")

        return create_struct(json_config['fields'])

    def load_schema_from_json(file_path):
        with open(file_path, "r") as json_file:
            schema_json = json.load(json_file)
        return StructType.fromJson(schema_json)


    def validate_schema(row):
        return set(row.keys()) == set(["ID", "name", "age", "address", "loaddate"])

    @staticmethod
    def validate_data_types(row):
        return isinstance(row["ID"], int) and isinstance(row["age"], int)

    @staticmethod
    def format_data(row):
        if isinstance(row["name"], str):
            row["name"] = row["name"].strip()

        if isinstance(row["address"], dict) and isinstance(row["address"].get("city"), str):
            row["address"]["city"] = row["address"]["city"].strip()

        return row

    @staticmethod
    def validate_and_format(row):
        if not commonUtil.validate_schema(row):
            return None  # Skip invalid schema

        if not commonUtil.validate_data_types(row):
            return None  # Skip invalid data types

        return commonUtil.format_data(row)

    @staticmethod
    def validate_and_format_list(rows):
        return [commonUtil.validate_and_format(row) for row in rows if commonUtil.validate_and_format(row) is not None]

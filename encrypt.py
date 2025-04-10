from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
import pyspark.sql.functions as spf
from cryptography.fernet import Fernet
from typing import Literal
from . import logs


class Encryptor(ABC):
    """
    Base class to hide implementation details.
    """

    @abstractmethod
    def encrypt(self, data: bytes) -> bytes | None:
        pass

    @abstractmethod
    def decrypt(self, data: bytes) -> bytes | None:
        pass


class FernetEncryptor(Encryptor):

    _key: str
    _fernet: Fernet

    def __init__(self, key: bytes | str):
        assert isinstance(key, (bytes, str))

        if isinstance(key, str):
            key = key.encode()

        self._fernet = Fernet(key)

    def encrypt(self, data: bytes) -> bytes | None:
        assert isinstance(data, bytes)

        return self._fernet.encrypt(data)

    def decrypt(self, data: bytes) -> bytes | None:
        assert isinstance(data, bytes)

        try:
            return self._fernet.decrypt(data)
        except Exception as ex:
            return None


def get_encrypt_udf(encryptor: Encryptor) -> callable:

    def encrypt_value(value: any) -> str | None:

        if value is None:
            return None

        value_str = str(value)
        encrypted_bytes = encryptor.encrypt(value_str.encode())

        if encrypted_bytes is None:
            raise RuntimeError(f"Failed to encrypt '{value_str}'.")

        encrypted_value = encrypted_bytes.decode()
        return encrypted_value

    return spf.udf(encrypt_value)


def get_decrypt_udf(encryptor: Encryptor) -> callable:
    def decrypt_value(value: str | None) -> str | None:

        assert isinstance(value, (str | None))

        if value is None:
            return None

        decrypted_bytes = encryptor.decrypt(value.encode())

        if decrypted_bytes is None:
            raise RuntimeError(f"Failed to decrypt value '{value}'.")

        return decrypted_bytes.decode()

    return spf.udf(decrypt_value)


def encrypt_table(
    df: DataFrame | ConnectDataFrame,
    columns: list[str],
    encryptor: Encryptor,
) -> DataFrame | ConnectDataFrame:

    encrypt_value_udf = get_encrypt_udf(encryptor)

    for column in columns:
        logs.log_info(f"Encrypting '{column}'...")
        df = df.withColumn(column, encrypt_value_udf(column).cast("string"))

    return df


def decrypt_table(
    df: DataFrame | ConnectDataFrame,
    columns: list[str],
    encryptor: Encryptor,
    on_error: Literal["raise"] = "raise",
) -> DataFrame | ConnectDataFrame:

    decrypt_value_udf = get_decrypt_udf(encryptor)

    for column in columns:
        logs.log_info(f"Decrypting '{column}'...")
        df = df.withColumn(column, decrypt_value_udf(column).cast("string"))

    return df

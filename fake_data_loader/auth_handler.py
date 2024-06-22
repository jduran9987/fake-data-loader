from pathlib import Path


class AuthHandler:
    """
    Read in an auth config file and convert into a
    dictionary.
    """
    @classmethod
    def convert_to_dict(cls, config_file: Path) -> dict:
        """
        Convert the config file to a dictionary.
        """
        if not config_file.exists():
            raise FileNotFoundError(f"No file found at {config_file}")

        results = {}

        with config_file.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                key, value = line.split("=", 1)
                results[key] = value

        return results

import sys
from src.utils.logging import logger

class CustomException(Exception):
    def __init__(self, error_message: str, error_details):
        """
        Custom exception to capture detailed error information including
        filename and line number for debugging.
        """
        self.error_message = error_message

        try:
            _, _, exc_tb = error_details.exc_info()
            self.lineno = exc_tb.tb_lineno if exc_tb else "Unknown"
            self.filename = exc_tb.tb_frame.f_code.co_filename if exc_tb else "Unknown"
        except Exception as e:
            self.lineno = "Unavailable"
            self.filename = "Unavailable"
            logger.error(f"Failed to extract traceback info: {e}")

        super().__init__(self.__str__())

    def __str__(self):
        return (
            f"Exception in '{self.filename}' at line {self.lineno}:\n"
            f"→ {self.error_message}"
        )

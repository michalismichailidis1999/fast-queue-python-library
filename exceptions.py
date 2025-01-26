class RetryableException(Exception):
    def __init__(self, error_code: int, error_message: str):
        self.error_code: int = error_code
        self.error_message: str = error_message

    def __str__(self):
        return self.error_message

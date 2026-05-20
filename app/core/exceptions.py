class BackpressureError(Exception):
    """Queue depth exceeded the high watermark."""
    pass


class JobNotFoundError(Exception):
    """No job exists with the given ID."""
    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f"Job {job_id} not found")


class InvalidPriorityError(Exception):
    """Client supplied an unknown priority value."""
    def __init__(self, value: str):
        self.value = value
        super().__init__(f"Invalid priority: {value!r}. Use critical, high, or normal")

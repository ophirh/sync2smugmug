import json
import pathlib

_OPTIMIZATION_CONTEXT = "optimization_context.json"


class Optimization:
    def __init__(self, base_dir: pathlib.Path):
        self.base_dir = base_dir
        self._ctx: dict[str, dict] = self._load_context()

    @property
    def context_key(self) -> str:
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    @property
    def my_context(self) -> dict:
        if self.context_key not in self._ctx:
            self._ctx[self.context_key] = {}

        # Always return a copy
        return self._ctx[self.context_key].copy()

    def save_context(self, new_context: dict):
        self._ctx[self.context_key] = new_context
        self._save_context()

    def _load_context(self) -> dict[str, dict]:
        """
        Load the full optimization context. We will only make the private context available for each class
        """
        context_file_path = self.base_dir.joinpath(_OPTIMIZATION_CONTEXT)
        if not context_file_path.exists():
            return {}

        with context_file_path.open() as f:
            return json.load(f)

    def _save_context(self):
        """
        Save the full optimization context to disk
        """
        context_file_path = self.base_dir.joinpath(_OPTIMIZATION_CONTEXT)
        with context_file_path.open(mode="w") as f:
            json.dump(self._ctx, f)

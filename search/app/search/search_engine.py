import pandas as pd
from rapidfuzz import process, fuzz
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

class SearchEngine:
    _instances = {}

    def __new__(cls, filepath, search_column_name, num_workers=4):
        if filepath not in cls._instances:
            instance = super(SearchEngine, cls).__new__(cls)
            cls._instances[filepath] = instance
            instance.search_column_name = search_column_name
            instance.init(filepath, num_workers)
        return cls._instances[filepath]

    def init(self, filepath, num_workers):
        """Initialize a new instance if it hasn't been created for the given filepath."""
        self.df = pd.read_csv(filepath)
        self.num_workers = num_workers
        self.batches = self._create_batches()
        self.stop_search = Event()

    def _create_batches(self):
        """Divide the DataFrame into batches for parallel processing."""
        batch_size = len(self.df) // self.num_workers
        return [self.df.iloc[i * batch_size:(i + 1) * batch_size]
                for i in range(self.num_workers)]

    def _search_batch(self, data_subset, query, score_cutoff=70):
        """Perform a fuzzy search on a subset of the data."""
        if self.stop_search.is_set():
            return None
        results = process.extractOne(query, data_subset[self.search_column_name], scorer=fuzz.WRatio, score_cutoff=score_cutoff)
        if results and results[1] >= score_cutoff:
            best_match_idx = results[2]
            if best_match_idx < len(data_subset):
                self.stop_search.set()
                return data_subset.iloc[best_match_idx].to_dict()
            else:
                print(f"Index out-of-bounds: {best_match_idx} not in [0, {len(data_subset) - 1}]")
        return None


    def search(self, query):
        """Search for the best match using parallel processing across batches."""
        self.stop_search.clear()
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self._search_batch, batch, query) for batch in self.batches]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    return result  # Return the first match found

        return None  # Return None if no match found after all batches are processed

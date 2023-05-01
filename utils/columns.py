from typing import Dict

class Columns():
    def __init__(self, columns_dict: Dict[str, str]) -> None:
        self.columns_dict = columns_dict
        self.kor = list(self.columns_dict.keys())
        self.eng = list(self.columns_dict.values())
    
    def __len__(self) -> int:
        return len(self.columns_dict)
    

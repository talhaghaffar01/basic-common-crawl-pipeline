from tokenizers import Tokenizer
from tokenizers.models import BPE
from tokenizers.trainers import BpeTrainer
from tokenizers.pre_tokenizers import Whitespace
from tokenizers.processors import TemplateProcessing
from typing import Dict, List, Union
import os

class DocumentTokenizer:
    def __init__(self, vocab_size: int = 50000):
        self.vocab_size = vocab_size
        self.tokenizer = self._initialize_tokenizer()

    def _initialize_tokenizer(self) -> Tokenizer:
        """Initialize a BPE tokenizer."""
        tokenizer = Tokenizer(BPE(unk_token="[UNK]"))
        tokenizer.pre_tokenizer = Whitespace()

        # Add special tokens and configure post-processing
        tokenizer.post_processor = TemplateProcessing(
            single="[CLS] $A [SEP]",
            pair="[CLS] $A [SEP] $B [SEP]",
            special_tokens=[
                ("[CLS]", 1),
                ("[SEP]", 2),
                ("[UNK]", 3),
                ("[PAD]", 4),
            ],
        )

        return tokenizer

    def train(self, texts: List[str], save_path: str = "tokenizer.json"):
        """Train the tokenizer on a list of texts."""
        trainer = BpeTrainer(
            vocab_size=self.vocab_size,
            special_tokens=["[UNK]", "[CLS]", "[SEP]", "[PAD]"],
            show_progress=True
        )

        self.tokenizer.train_from_iterator(texts, trainer=trainer)
        self.tokenizer.save(save_path)

    def load(self, path: str = "tokenizer.json"):
        """Load a pre-trained tokenizer."""
        if os.path.exists(path):
            self.tokenizer = Tokenizer.from_file(path)
        else:
            raise FileNotFoundError(f"Tokenizer file not found at {path}")

    def tokenize(self, text: str) -> Dict[str, Union[List[int], List[str]]]:
        """Tokenize text and return both ids and tokens."""
        encoding = self.tokenizer.encode(text)
        return {
            'ids': encoding.ids,
            'tokens': encoding.tokens,
            'attention_mask': [1] * len(encoding.ids)
        }
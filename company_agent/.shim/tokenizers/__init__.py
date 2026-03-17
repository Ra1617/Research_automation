class Tokenizer:
    @classmethod
    def from_pretrained(cls, _name):
        return cls()

    def encode_batch(self, texts):
        return [list(text) for text in texts]

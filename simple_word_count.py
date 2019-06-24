import re
from mrjob.job import MRJob

WORD_REGEX = re.compile(r"[\w]+")

class WordCounter(MRJob):
    def mapper(self, _, line):
        for word in WORD_REGEX.findall(line):
            yield(word.lower(), 1)

    def combiner(self, word, counts):
        yield(word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    WordCounter.run()
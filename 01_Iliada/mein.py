from mrjob.job import MRJob
import re

WORD_RE = re.compile(r'[\w]+')


class MRMostCommonWord(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == "__main__":
    MRMostCommonWord.run()
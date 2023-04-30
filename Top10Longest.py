from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import json


class LongestTitles(MRJob):

    SHOW_LIMIT = 10
    MIN_COUNT = 10

    def rating(self, movie_id):
        '''
        Convert from movie id to rating
        '''
        with open("/root/input/u.data", "r", encoding="ISO-8859-1") as infile:
            reader = csv.reader(infile, delimiter='\t')
            next(reader)
            ratings = []
            for line in reader:
                if int(movie_id) == int(line[1]):
                    ratings.append(int(line[2]))
            return ratings

    def steps(self):
        '''
        Pipeline of MapReduce tasks
        '''
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]

    def mapper1(self, _, line):
        (movie_id, movie_title, *_) = line.split('|')
        yield movie_id, movie_title

    def reducer1(self, movie_id, movie_titles):
        movie_title = json.dumps(list(movie_titles))
        ratings = self.rating(movie_id)
        if len(ratings) >= self.MIN_COUNT:
            title_length = len(movie_title)
            yield movie_id, (movie_title, title_length)

    def mapper2(self, movie_id, title_length):
        yield None, (title_length, movie_id)

    def reducer2(self, _, values):
        i = 0
        for title_length, movie_id in sorted(values, reverse=True):
            i += 1
            if i <= self.SHOW_LIMIT:
                yield movie_id, (title_length[0], title_length[1])


if __name__ == '__main__':
    LongestTitles.run()

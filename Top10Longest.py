from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import json


class LongestTitles(MRJob):

    SHOW_LIMIT = 10
    MIN_COUNT = 10

    def ratings(self, movie_id):
        '''
        Convert from movie id to list of ratings
        '''
        ratings_list = []
        with open("/root/input/u.data", "r", encoding="ISO-8859-1") as infile:
            reader = csv.reader(infile, delimiter='\t')
            next(reader)
            for line in reader:
                if int(movie_id) == int(line[1]):
                    ratings_list.append(line[2])
        return ratings_list

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
        yield movie_title, movie_id

    def reducer1(self, movie_title, movie_id):
        movie_id_list = list(movie_id)
        if len(self.ratings(movie_id_list[0])) >= self.MIN_COUNT:
            title_length = len(movie_title)
            yield title_length, movie_title

    def mapper2(self, title_length, movie_title):
        yield None, (title_length, movie_title)

    def reducer2(self, _, values):
        i = 0
        for title_length, movie_title in sorted(values, reverse=True):
            i += 1
            if i <= self.SHOW_LIMIT:
                yield movie_title, title_length


if __name__ == '__main__':
    LongestTitles.run()

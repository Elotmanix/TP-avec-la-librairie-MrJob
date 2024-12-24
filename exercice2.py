from mrjob.job import MRJob
import re


class SingleVowelWords(MRJob):
    def mapper(self, _ , line):
        word = line.strip().lower()
        vowels =set('aeiouy')
        # Trouver toutes les voyelles uniques dans le mot
        vowels_in_word =set(char for char in word if char in vowels)  
        # Si le mot contient exactement une voyelle (peut-être répété)
        if len(vowels_in_word)== 1:
            vowel= vowels_in_word.pop()  
            yield vowel, (word, len(word))

    def reducer(self, vowel, word_length_pairs):
        words= list(word_length_pairs)
        max_length= max(length for word, length in words)
        longest_words =[word for word, length in words if length ==max_length]
        yield vowel, {
            'longest_length':  max_length,
            'words': longest_words
        }


if __name__ == '__main__' :
    SingleVowelWords.run()
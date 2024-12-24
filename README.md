# Sales Data Analysis

## Preparation
Create a directory named `ventes` on your machine, and copy the file `sales_data_sample.csv` (2824 rows) into it.

## Exercise 1 - Querying a Sales File
The file is organized into 20 columns, separated by a tabulation.

### Notes:
- The first line of the file contains column headers. You need to exclude it from processing.
- Some rows in the file contain incomplete columns or malformed dates. Your program should ignore these rows.

### Questions to Implement Using the MRJob Library

1. **List all product categories (PRODUCTLINE) present in the file.**
2. **Determine the total sales quantity (QUANTITYORDERED) for each product category.**
3. **Calculate the revenue (based on QUANTITYORDERED and PRICEEACH) by year and product category (across all countries).**
4. **For each country, identify the store (CUSTOMERNAME) that achieved the highest revenue in the "Trucks and Buses" category.**
5. **Add an original and complex query (requiring more than one processing step) of your choice on this file.** Include the French formulation of this query as a comment in your Python script.

### Execution Example
The execution of a query is performed as follows:

```bash
python requete1.py -r inline sales_data_sample.csv > requete1.txt
```

## Exercise 2 - Lexical Particularity
Given a file of words, write an MRJob script to detect the longest words that contain only one vowel (from the set `aeiouy`), possibly appearing multiple times.

For example, in a French word dictionary, the word `abracadabrant` (13 letters) contains only the vowel `a` (appearing 5 times).

### Output
The output should display such words for each of the 6 vowels. The algorithm should disregard the presence of uppercase letters in the words.

### Word Dictionaries
For intensive testing, you can use the English-language file `Dictionnaire.txt`.

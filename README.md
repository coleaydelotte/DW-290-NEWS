# DW-290-NEWS

## Collaborators

- Jeffery Eisenhardt: eisenhardtj
- Cole Aydelotte: coleaydelotte
- Harrison Krauss: kraussh-art

## How it works
This program uses DuckDB to combine all the json 
files into one CSV file that then feeds into pandas
where queries are ran.  
The DuckDB queries are the Author, title, the site where it originates from,
the published date, the country of origin, and language of the
article, and the categories from the website.
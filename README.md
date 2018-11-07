This scraper captures the current cannabis product information from
[the Ontario Cannabis Store
website](https://ocs.ca/collections/all-cannabis-products).

To get the data scraped so far, visit
<https://morph.io/OddBloke/ontario_cannabis_store_scraper>.

## The Database

The database produced by the scraper (and available for download
[here](https://morph.io/OddBloke/ontario_cannabis_store_scraper)) has
two tables.  `data` contains the results from the most recent scrape,
for immediate access.  `history` contains all the result we've ever
scraped; these are in the same format as `data` with a `timestamp`
column added.  The timestamp will be the same for a particular scraping
run.

## Useful Queries

This will show you the products added between the most recent scrape
and the one before it:

```sql
SELECT url
FROM   history
WHERE  timestamp = (SELECT DISTINCT timestamp
                    FROM   history
                    ORDER  BY timestamp DESC
                    LIMIT  1)
       AND url NOT IN (SELECT url
                       FROM   history
                       WHERE  timestamp = (SELECT DISTINCT timestamp
                                           FROM   history
                                           ORDER  BY timestamp DESC
                                           LIMIT  1, 1));
```

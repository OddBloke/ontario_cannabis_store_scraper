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

If you download the database from
[morph.io](https://morph.io/OddBloke/ontario_cannabis_store_scraper),
then you can run the following queries using SQLite locally.
Alternatively, morph.io do provide an API for running queries; you can
find it
[here](https://morph.io/documentation/api?scraper=OddBloke%2Fontario_cannabis_store_scraper).

### Recently Added Products

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

### Highest THC/CBD Products

This will show you the 5 highest average THC/$ dried flower products:

```sql
SELECT name,
       ( thc_high + thc_low ) / 2 / price AS value,
       url
FROM   data
WHERE  type = "Dried Flowers"
ORDER  BY value DESC
LIMIT  5;
```

And we can do the same for CBD:

```sql
SELECT name,
       ( cbd_high + cbd_low ) / 2 / price AS value,
       url
FROM   data
WHERE  type = "Dried Flowers"
ORDER  BY value DESC
LIMIT  5;
```

(Note that these queries won't work if you include other product types,
because the weights for other products are variable.)

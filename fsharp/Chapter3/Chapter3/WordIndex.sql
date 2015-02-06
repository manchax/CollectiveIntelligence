--CREATE TABLE WordsLinks_Positions (
--	ID int IDENTITY(1,1) PRIMARY KEY,
--	WordLinkID int NOT NULL FOREIGN KEY REFERENCES WordsLinks(ID),
--	Position int NOT NULL
--);
SELECT 
(SELECT count(*) FROM Words) as UniqueWords,
(SELECT count(*) FROM Links) as Links,
(SELECT count(*) FROM WordsLinks) as UniqueWordsPerLink,
(SELECT count(*) FROM WordsLinksPositions) as TotalWords,
(SELECT AVG(DATALENGTH(Word)) FROM Words) as AvgWordLength

-- PAGES WITH SPECIFIC WORD
SELECT wl.WordID, wl.LinkID, l.Link, wl.Count, w.Word, wl.ID
--,wlp.WordLinkID, wlp.Position
FROM Links l
INNER JOIN WordsLinks wl ON wl.LinkID = l.ID
inner join Words w ON wl.WordID = w.ID
--INNER JOIN WordsLinksPositions wlp ON wlp.WordLinkID = wl.ID
WHERE (w.Word = 'MÉXICO' or w.Word = 'Oribe') 
and l.Link = 'http://www.eluniversal.com.mx/finanzas-cartera/2014/solicitudes-hipotecas-eu-1033466.html'
ORDER BY Count desc

-- duplicates
SELECT wl.WordID, wl.LinkID, count(*)
FROM WordsLinks wl
GROUP BY  wl.WordID, wl.LinkID
having count(*) > 1

-- PAGES WITH SPECIFIC WORD
SELECT wl.WordID, wl.LinkID, l.Link, wl.Count, w.Word, wl.ID
--,wlp.WordLinkID, wlp.Position
FROM Links l
INNER JOIN WordsLinks wl ON wl.LinkID = l.ID
inner join Words w ON wl.WordID = w.ID
--INNER JOIN WordsLinksPositions wlp ON wlp.WordLinkID = wl.ID
ORDER BY Count desc

-- TOP 50 WORDS BY REP
SELECT TOP 50 w.Word, sum(wl.Count)
FROM Links l
INNER JOIN WordsLinks wl ON wl.LinkID = l.ID
inner join Words w ON wl.WordID = w.ID
group by w.Word
ORDER BY 2 desc




use project_dbms;

CREATE TABLE hobby (
    email VARCHAR(100),
    hobby VARCHAR(100),
    PRIMARY KEY (email, hobby),
    FOREIGN KEY (email) REFERENCES user(email)
);

CREATE TABLE follow (
    follower VARCHAR(100),
    followee VARCHAR(100),
    PRIMARY KEY (follower, followee),
    FOREIGN KEY (follower) REFERENCES user(email),
    FOREIGN KEY (followee) REFERENCES user(email)
);

CREATE TABLE vendor (
    id INTEGER,
    name VARCHAR(100),
    PRIMARY KEY (id)
);

SHOW TABLES;

CREATE TABLE vendor_ambassador (
    vendorid INTEGER,
    email VARCHAR(100),
    PRIMARY KEY (vendorid),
    FOREIGN KEY (email) REFERENCES user(email),
    FOREIGN KEY (vendorid) REFERENCES vendor(id)
);


CREATE TABLE topic (
    id INTEGER,
    description VARCHAR(100),
    PRIMARY KEY (id)
);

CREATE TABLE vendor_topics (
    vendorid INTEGER,
    topicid INTEGER,
    PRIMARY KEY (vendorid, topicid),
    FOREIGN KEY (vendorid) REFERENCES vendor(id),
    FOREIGN KEY (topicid) REFERENCES topic(id)
);


CREATE TABLE blurt_analysis (
    email VARCHAR(100),
    blurtid INTEGER,
    topicid INTEGER,
    confidence DECIMAL(5,2),
    sentiment INTEGER,
    PRIMARY KEY (email, blurtid, topicid),
    FOREIGN KEY (email, blurtid) REFERENCES blurt(email, blurtid),
    FOREIGN KEY (topicid) REFERENCES topic(id),
    CHECK (confidence >= 0 AND confidence <= 10),
    CHECK (sentiment >= -5 AND sentiment <= 5)
);



CREATE TABLE advertisement (
    id INTEGER,
    content VARCHAR(255),
    vendorid INTEGER,
    PRIMARY KEY (id),
    FOREIGN KEY (vendorid) REFERENCES vendor(id)
);

CREATE TABLE user_ad (
    email VARCHAR(100),
    adid INTEGER,
    PRIMARY KEY (email, adid),
    FOREIGN KEY (email) REFERENCES user(email)
);

ALTER TABLE user_ad
ADD CONSTRAINT fk_adid FOREIGN KEY (adid) REFERENCES advertisement(id);


show tables;

SELECT * FROM user;
SELECT * FROM hobby;
SELECT * FROM follow;
SELECT * FROM vendor;
SELECT * FROM vendor_ambassador;
SELECT * FROM topic;
SELECT * FROM vendor_topics;
SELECT * FROM blurt;
SELECT * FROM blurt_analysis;
SELECT * FROM advertisement;
SELECT * FROM user_ad;
SELECT * FROM celebrity;

/*--query1--*/
SELECT h.hobby, COUNT(u.email) AS total_users
FROM hobby h
JOIN user u ON h.email = u.email
GROUP BY h.hobby;
select hobby,count('email') as total from hobby group by hobby.hobby ; 

/*--query2--*/
SELECT u.name, u.address
FROM user u
JOIN celebrity c ON u.email = c.email
WHERE c.kind <> 'movieStar';
select name,address from user u join celebrity c where u.email=c.email and u.type ='C' and c.kind !='MovieStar'; 

/*--query3--*/
SELECT v.name, COUNT(a.id) AS num_ads
FROM vendor v
JOIN advertisement a ON v.id = a.vendorid
GROUP BY v.name
ORDER BY num_ads DESC;

use project_dbms;
select * from user;

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1; -- Optionally re-enable it

delete from blurt_analysis;
select * from blurt_analysis;

/*--query4--*/
SELECT u.email, u.password, u.name, u.date_of_birth, c.kind
FROM user u
JOIN celebrity c ON u.email = c.email
JOIN follow f ON u.email = f.follower
WHERE c.kind = 'movieStar';

/*--query5--*/
SELECT u.name, u.email, COUNT(f.followee) AS num_followers
FROM user u
JOIN celebrity c ON u.email = c.email
JOIN follow f ON c.email = f.followee
GROUP BY u.name, u.email
HAVING num_followers > 55
ORDER BY num_followers DESC;


/*--query6--*/
SELECT distinct ua.name AS follower_name, ub.name AS followee_name
FROM user ua
JOIN follow f ON ua.email = f.follower
JOIN user ub ON f.followee = ub.email
WHERE NOT EXISTS (
    SELECT 1
    FROM blurt_analysis ba1
    JOIN blurt_analysis ba2 ON ba1.topicid = ba2.topicid
    WHERE ba1.email = ua.email AND ba2.email = ub.email
);


/*--query7--*/
SELECT location, COUNT(blurtid) AS total_blurts
FROM blurt
GROUP BY location
HAVING total_blurts > 40
ORDER BY total_blurts DESC;

/*--query8--*/
SELECT v.name AS vendor_name, va.email AS ambassador_email, COUNT(f.follower) AS num_following
FROM vendor v
JOIN vendor_ambassador va ON v.id = va.vendorid
JOIN follow f ON va.email = f.follower
GROUP BY v.name, va.email
ORDER BY num_following DESC;


/*--query9--*/
SELECT v.id AS vendor_id, v.name AS vendor_name, va.email AS ambassador_email, COUNT(ua.email) AS total_users_shown_ads
FROM vendor v
JOIN vendor_ambassador va ON v.id = va.vendorid
JOIN advertisement a ON v.id = a.vendorid
JOIN user_ad ua ON a.id = ua.adid
GROUP BY v.id, v.name, va.email;













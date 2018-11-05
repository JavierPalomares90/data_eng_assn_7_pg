/** COPY TABLES **/
CREATE TABLE A AS
SELECT * FROM testdata;

CREATE TABLE B AS
SELECT * FROM testdata;

CREATE TABLE C AS
SELECT * FROM testdata;

CREATE TABLE A_prime AS
SELECT * FROM testdata;

CREATE TABLE B_prime AS
SELECT * FROM testdata;

CREATE TABLE C_prime AS
SELECT * FROM testdata;

/**CREATE INDICES **/
CREATE INDEX a_pk_index on A_prime(pk);
CREATE INDEX a_ht_index on A_prime(ht);
CREATE INDEX a_tt_index on A_prime(tt);
CREATE INDEX a_ot_index on A_prime(ot);
CREATE INDEX a_hund_index on A_prime(hund);
CREATE INDEX a_ten_index on A_prime(ten);

CREATE INDEX b_pk_index on B_prime(pk);
CREATE INDEX b_ht_index on B_prime(ht);
CREATE INDEX b_tt_index on B_prime(tt);
CREATE INDEX b_ot_index on B_prime(ot);
CREATE INDEX b_hund_index on B_prime(hund);
CREATE INDEX b_ten_index on B_prime(ten);

CREATE INDEX c_pk_index on C_prime(pk);
CREATE INDEX c_ht_index on C_prime(ht);
CREATE INDEX c_tt_index on C_prime(tt);
CREATE INDEX c_ot_index on C_prime(ot);
CREATE INDEX c_hund_index on C_prime(hund);
CREATE INDEX c_ten_index on C_prime(ten);


/** SECTION 1 **/
/*1*/
SELECT * FROM A JOIN B ON A.pk =B.pk;

/*2*/
SELECT * FROM A JOIN B ON A.ht =B.ht;

/*3*/
SELECT * FROM A_prime JOIN B_prime ON A_prime.ht =B_prime.ht;

/*4*/
SELECT * FROM A JOIN B ON A.ten =B.ten;

/*5*/
SELECT * FROM A_prime JOIN B_prime ON A_prime.ten =B_prime.ten;

/*6*/
SELECT * FROM A JOIN B ON A.ht =B.ten;

/*7*/
SELECT * FROM B JOIN A ON B.ten =A.ht;

/*8*/
SELECT * FROM A JOIN B_prime ON A.ht =B_prime.ten;

/*9*/
SELECT * FROM B_prime JOIN A ON B_prime.ten = A.ht;

/* 3 way joins */
/*10*/
SELECT * FROM A JOIN B ON A.ht = B.ht JOIN C ON B.ht = C.ht;

/*11*/
SELECT * FROM A JOIN B ON A.ten = B.ten JOIN C ON B.ten = C.ten;

/*12 TODO: Check if 10 and 11 produce different results*/
/** 10 chooses hash join to join a,b, then c. 
    11 chooses merge join to join a,b, then hash join to join c. 
 Plan changes at ot attribute
 SELECT * FROM A JOIN B ON A.ot = B.ot JOIN C ON B.ot = C.ot; **/

/*13*/
SELECT * FROM A JOIN B ON A.pk = B.ht JOIN C ON B.ht = C.ht

/*14*/
SELECT * FROM A JOIN B ON A.pk = B.hund JOIN C ON B.hund = C.hund

/*15 TODO: Check if 13 and 14 produce different results */
/** 13 hash joins a,b then hashjoins c. 
    14 hash joins a,b then hashjoincs c. 13 and 14 produce the same plan **/

/*16*/
SELECT * FROM A_prime JOIN B_prime ON A_prime.ht = B_prime.ht JOIN C_prime ON B_prime.ht = C_prime.ht

/*17*/
SELECT * FROM A_prime JOIN B_prime ON A_prime.hund = B_prime.hund JOIN C_prime ON B_prime.hund = C_prime.hund

/*18 */ 
SELECT * FROM A_prime JOIN B_prime ON A_prime.ht = B_prime.ten JOIN C_prime ON B_prime.ten = C_prime.ht

/*19 */
SELECT * FROM A_prime,C_prime,A,B,B_prime,C 
WHERE 
A_prime.ht = C_prime.ot AND
A.ht = A_prime.ten AND
B.pk = C_prime.hund AND
A_prime.ht = C_prime.ot AND
B_prime.ten = C.ot AND
B_prime.hund = A.ht 

/*20*/
SELECT * FROM A_prime,C_prime,A,B
WHERE 
A_prime.ht = C_prime.ot AND
A_prime.ten = 5 AND
A.ht < A_prime.ten AND
B.pk = C_prime.hund AND
B.ot < 500 AND
A_prime.ht = C_prime.ot

/*21*/
SELECT * FROM A 
JOIN B ON A.ht = B.tt
JOIN C ON B.tt = C.ot

/*22*/
SELECT * FROM C
JOIN B ON C.ot = B.tt
JOIN A ON B.tt = A.ht

/*23*/
SELECT * FROM A
JOIN B_prime ON A.pk = B_prime.tt
JOIN C_prime ON B_primt.tt = C_prime.ot

/*24*/
SELECT *FROM C_prime
JOIN B_prime ON C_prime.ot = B_prime.tt
JOIN A ON B_prime.tt = A.pk



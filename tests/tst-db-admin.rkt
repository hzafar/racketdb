#lang racket

(require
  "../racketdb.rkt"
  rackunit
  rackunit/log)

(let ([c (connect)])
  (let ([result (first (run c (db-create "library")))])
    (check-equal? (hash-ref result 'dbs_created) 1))

  (let ([result (first (run c (db "library") (table-create "books")))])
    (check-equal? (hash-ref result 'tables_created) 1))

  (let ([result (first (run c (db-list)))])
    (check-not-false (member "library" result)))

  (let ([result (first (run c (db "library") (table-list)))])
    (check-not-false (member "books" result)))

  (let ([result (first (run c (db "library") (table-drop "books")))])
    (check-equal? (hash-ref result 'tables_dropped) 1))

  (let ([result (first (run c (db-drop "library")))])
    (check-equal? (hash-ref result 'dbs_dropped) 1))

  (close-connection c))

(test-log #:display? #t #:exit? #t)

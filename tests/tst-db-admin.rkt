#lang racket

(require
  "../racketdb.rkt"
  rackunit
  rackunit/log)

(let ([c (connect)])
  (if (member "library" (first (run (db-list) with c)))
      (run (db-drop "library") with c)
      (void))

  (let ([result (first (run (db-create "library") with c))])
    (check-equal? (hash-ref result 'dbs_created) 1))

  (let ([result (first (run (db "library") (table-create "books") with c))])
    (check-equal? (hash-ref result 'tables_created) 1))

  (let ([result (first (run (db-list) with c))])
    (check-not-false (member "library" result)))

  (let ([result (first (run (db "library") (table-list) with c))])
    (check-not-false (member "books" result)))

  (let ([result (first (run (db "library") (table-drop "books") with c))])
    (check-equal? (hash-ref result 'tables_dropped) 1))

  (let ([result (first (run (db-drop "library") with c))])
    (check-equal? (hash-ref result 'dbs_dropped) 1))

  (close-connection c))

(test-log #:display? #t #:exit? #t)

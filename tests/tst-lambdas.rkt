#lang racket

(require
  "../racketdb.rkt"
  rackunit
  rackunit/log)

(define (book title author)
  (hash "title" title "author" author))

;; setup
(let ([c (connect)])
  (if (member "library" (first (run (db-list) with c)))
      (run (db-drop "library") with c)
      (void))
  (run (db-create "library") with c)
  (run (db "library") (table-create "books" with (hash "primary_key" "title")) with c)

  (let ([result (run (db "library")
                     (table "books")
                     (insert (array
                               (book "A Canticle for Leibowitz" "Walter M. Miller Jr.")
                               (book "Mad Shadows" "Marie-Claire Blais")
                               (book "The Sirian Experiments" "Doris Lessing")
                               (book "The Manticore" "Robertson Davies")
                               (book "Murther and Walking Spirits" "Robertson Davies")
                               (book "The Sentimental Agents in the Volyen Empire" "Doris Lessing")
                               (book "Shikasta" "Doris Lessing")
                               (book "Three Plays About Crime and Criminals" "Kesselring, Kingsley, and Chodorov")
                               (book "By Grand Central Station I Sat Down and Wept" "Elizabeth Smart")))
                     with c)])
    (check-equal? (hash-ref (first result) 'inserted) 9))

  ;; very simple demonstration of ReQL lambda
  (let ([result (run (db "library")
                     (table "books")
                     (r:filter (r:lambda (x) (datum x) (get-field "author") (eq "Doris Lessing")))
                     (count)
                  with c)])
    (check-equal? (first result) 3))

  (close-connection c))

(test-log #:display? #t #:exit? #t)

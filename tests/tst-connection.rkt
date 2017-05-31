#lang racket

(require
  "../racketdb.rkt"
  rackunit
  rackunit/log)

(let ([c (connect)])
  (check-equal? (length c) 3)
  (check-not-exn (λ () (close-input-port (first c))))
  (check-not-exn (λ () (close-output-port (second c)))))

(test-log #:display? #t #:exit? #t)

#lang racket

(require "../pbkdf2.rkt"
         rackunit
         rackunit/log)

;; PBKDF2 Tests

; Test vectors from: https://stackoverflow.com/questions/5130513/pbkdf2-hmac-sha2-test-vectors/5130543#5130543

(check-equal?
  (pbkdf2 hmac-sha256 #"password" #"salt" 1 20)
  (bytes #x12 #x0f #xb6 #xcf #xfc #xf8 #xb3 #x2c #x43 #xe7 #x22 #x52 #x56 #xc4 #xf8 #x37 #xa8 #x65 #x48 #xc9))

(check-equal?
  (pbkdf2 hmac-sha256 #"password" #"salt" 2 20)
  (bytes #xae #x4d #x0c #x95 #xaf #x6b #x46 #xd3 #x2d #x0a #xdf #xf9 #x28 #xf0 #x6d #xd0 #x2a #x30 #x3f #x8e))

(check-equal?
  (pbkdf2 hmac-sha256 #"password" #"salt" 4096 20)
  (bytes #xc5 #xe4 #x78 #xd5 #x92 #x88 #xc8 #x41 #xaa #x53 #x0d #xb6 #x84 #x5c #x4c #x8d #x96 #x28 #x93 #xa0))

; This one takes a very long time to run. Uncomment if you have a couple hours.
;(check-equal?
;  (pbkdf2 hmac-sha256 #"password" #"salt" 16777216 20)
;  (bytes #xcf #x81 #xc6 #x6f #xe8 #xcf #xc0 #x4d #x1f #x31 #xec #xb6 #x5d #xab #x40 #x89 #xf7 #xf1 #x79 #xe8))

(check-equal?
  (pbkdf2 hmac-sha256 #"passwordPASSWORDpassword" #"saltSALTsaltSALTsaltSALTsaltSALTsalt" 4096 25)
  (bytes #x34 #x8c #x89 #xdb #xcb #xd3 #x2b #x2f #x32 #xd8 #x14 #xb8 #x11 #x6e #x84 #xcf #x2b #x17 #x34 #x7e #xbc #x18 #x00 #x18 #x1c))

(check-equal?
  (pbkdf2 hmac-sha256 #"pass\0word" #"sa\0lt" 4096 16)
  (bytes #x89 #xb6 #x9d #x05 #x16 #xf8 #x29 #x89 #x3c #x69 #x62 #x26 #x65 #x0a #x86 #x87))

(test-log #:display? #t #:exit? #t)

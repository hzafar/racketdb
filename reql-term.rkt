#lang racket

(provide (struct-out reql-term))

(struct reql-term (type args optargs) #:prefab)

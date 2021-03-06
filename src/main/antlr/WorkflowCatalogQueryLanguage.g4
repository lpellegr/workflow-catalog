grammar WorkflowCatalogQueryLanguage;

@header {
   package org.ow2.proactive.workflow_catalog.rest.query.parser;
}

// PARSER

expression
    : and_expression
    ;

and_expression
    : or_expression (OR or_expression)*
    ;

or_expression
    : clause (AND clause)*
    ;

clause
    : (AttributeLiteral COMPARE_OPERATOR StringLiteral) #finalClause
    | LPAREN and_expression RPAREN #parenthesedClause
    ;

// LEXER

AND                 : 'AND' | '&&' ;
OR                  : 'OR' | '||' ;
COMPARE_OPERATOR    : '!=' | '=' ;
LPAREN              : '(' ;
RPAREN              : ')' ;

StringLiteral
    : '"' (~["\\\r\n] | '\\' (. | EOF))* '"'
    ;

AttributeLiteral
    : LETTER (LETTER | DIGIT | '_' | '.')*
    ;

WS
    : [ \t\r\n]+ -> skip
    ;

fragment DIGIT: [0-9];
fragment LETTER: LOWERCASE | UPPERCASE;
fragment LOWERCASE: [a-z];
fragment UPPERCASE: [A-Z];

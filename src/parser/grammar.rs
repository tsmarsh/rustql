//! SQL Grammar/Parser
//!
//! A recursive descent parser for SQL statements. Uses the tokenizer
//! to read tokens and builds AST nodes according to SQL grammar.

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::*;
use crate::parser::tokenizer::{tokenize, Token, TokenKind};

// ============================================================================
// Parser
// ============================================================================

/// SQL parser
pub struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    pos: usize,
}

impl<'a> Parser<'a> {
    /// Create a new parser for the given SQL source
    pub fn new(source: &'a str) -> Result<Self> {
        let tokens = tokenize(source)?;
        Ok(Parser {
            source,
            tokens,
            pos: 0,
        })
    }

    /// Parse a single SQL statement
    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        self.skip_semicolons();

        if self.is_eof() {
            return Err(self.error("expected statement"));
        }

        let stmt = match self.current().kind {
            TokenKind::Explain => self.parse_explain(),
            TokenKind::Select | TokenKind::Values => Ok(Stmt::Select(self.parse_select_stmt()?)),
            TokenKind::With => self.parse_with_stmt(),
            TokenKind::Insert | TokenKind::Replace => Ok(Stmt::Insert(self.parse_insert_stmt()?)),
            TokenKind::Update => Ok(Stmt::Update(self.parse_update_stmt()?)),
            TokenKind::Delete => Ok(Stmt::Delete(self.parse_delete_stmt()?)),
            TokenKind::Create => self.parse_create(),
            TokenKind::Drop => self.parse_drop(),
            TokenKind::Alter => self.parse_alter(),
            TokenKind::Begin => self.parse_begin(),
            TokenKind::Commit | TokenKind::End => {
                self.advance();
                if self.check(TokenKind::Transaction) {
                    self.advance();
                }
                Ok(Stmt::Commit)
            }
            TokenKind::Rollback => self.parse_rollback(),
            TokenKind::Savepoint => {
                self.advance();
                let name = self.expect_identifier()?;
                Ok(Stmt::Savepoint(name))
            }
            TokenKind::Release => {
                self.advance();
                self.match_token(TokenKind::Savepoint);
                let name = self.expect_identifier()?;
                Ok(Stmt::Release(name))
            }
            TokenKind::Pragma => Ok(Stmt::Pragma(self.parse_pragma()?)),
            TokenKind::Vacuum => Ok(Stmt::Vacuum(self.parse_vacuum()?)),
            TokenKind::Analyze => self.parse_analyze(),
            TokenKind::Reindex => self.parse_reindex(),
            TokenKind::Attach => Ok(Stmt::Attach(self.parse_attach()?)),
            TokenKind::Detach => {
                self.advance();
                self.match_token(TokenKind::Database);
                let name = self.expect_identifier()?;
                Ok(Stmt::Detach(name))
            }
            _ => Err(self.error("expected statement")),
        }?;

        self.skip_semicolons();
        Ok(stmt)
    }

    /// Check if at end of file
    pub fn is_eof(&self) -> bool {
        self.current().kind == TokenKind::Eof
    }

    // ========================================================================
    // Statement Parsers
    // ========================================================================

    fn parse_explain(&mut self) -> Result<Stmt> {
        self.advance(); // EXPLAIN

        let query_plan = self.match_token(TokenKind::Query) && self.match_token(TokenKind::Plan);

        let stmt = self.parse_stmt()?;

        if query_plan {
            Ok(Stmt::ExplainQueryPlan(Box::new(stmt)))
        } else {
            Ok(Stmt::Explain(Box::new(stmt)))
        }
    }

    fn parse_with_stmt(&mut self) -> Result<Stmt> {
        let with = self.parse_with_clause()?;

        match self.current().kind {
            TokenKind::Select | TokenKind::Values => {
                let mut stmt = self.parse_select_stmt()?;
                stmt.with = Some(with);
                Ok(Stmt::Select(stmt))
            }
            TokenKind::Insert | TokenKind::Replace => {
                let mut stmt = self.parse_insert_stmt()?;
                stmt.with = Some(with);
                Ok(Stmt::Insert(stmt))
            }
            TokenKind::Update => {
                let mut stmt = self.parse_update_stmt()?;
                stmt.with = Some(with);
                Ok(Stmt::Update(stmt))
            }
            TokenKind::Delete => {
                let mut stmt = self.parse_delete_stmt()?;
                stmt.with = Some(with);
                Ok(Stmt::Delete(stmt))
            }
            _ => Err(self.error("expected SELECT, INSERT, UPDATE, or DELETE after WITH")),
        }
    }

    fn parse_with_clause(&mut self) -> Result<WithClause> {
        self.expect(TokenKind::With)?;
        let recursive = self.match_token(TokenKind::Recursive);

        let mut ctes = vec![self.parse_cte()?];
        while self.match_token(TokenKind::Comma) {
            ctes.push(self.parse_cte()?);
        }

        Ok(WithClause { recursive, ctes })
    }

    fn parse_cte(&mut self) -> Result<CommonTableExpr> {
        let name = self.expect_identifier()?;

        let columns = if self.match_token(TokenKind::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(TokenKind::As)?;

        let materialized = if self.match_token(TokenKind::Materialized) {
            Some(true)
        } else if self.match_token(TokenKind::Not) {
            self.expect(TokenKind::Materialized)?;
            Some(false)
        } else {
            None
        };

        self.expect(TokenKind::LParen)?;
        let query = Box::new(self.parse_select_stmt()?);
        self.expect(TokenKind::RParen)?;

        Ok(CommonTableExpr {
            name,
            columns,
            materialized,
            query,
        })
    }

    // ========================================================================
    // SELECT Statement
    // ========================================================================

    fn parse_select_stmt(&mut self) -> Result<SelectStmt> {
        let body = self.parse_select_body()?;

        let order_by = if self.match_token(TokenKind::Order) {
            self.expect(TokenKind::By)?;
            Some(self.parse_ordering_terms()?)
        } else {
            None
        };

        let limit = if self.match_token(TokenKind::Limit) {
            Some(self.parse_limit_clause()?)
        } else {
            None
        };

        Ok(SelectStmt {
            with: None,
            body,
            order_by,
            limit,
        })
    }

    fn parse_select_body(&mut self) -> Result<SelectBody> {
        let mut left = if self.match_token(TokenKind::Values) {
            SelectBody::Select(self.parse_values_core()?)
        } else {
            SelectBody::Select(self.parse_select_core()?)
        };

        // Handle compound operators
        loop {
            let op = if self.match_token(TokenKind::Union) {
                if self.match_token(TokenKind::All) {
                    CompoundOp::UnionAll
                } else {
                    CompoundOp::Union
                }
            } else if self.match_token(TokenKind::Intersect) {
                CompoundOp::Intersect
            } else if self.match_token(TokenKind::Except) {
                CompoundOp::Except
            } else {
                break;
            };

            let right = if self.match_token(TokenKind::Values) {
                SelectBody::Select(self.parse_values_core()?)
            } else {
                SelectBody::Select(self.parse_select_core()?)
            };

            left = SelectBody::Compound {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_select_core(&mut self) -> Result<SelectCore> {
        self.expect(TokenKind::Select)?;

        let distinct = if self.match_token(TokenKind::Distinct) {
            Distinct::Distinct
        } else {
            self.match_token(TokenKind::All);
            Distinct::All
        };

        let columns = self.parse_result_columns()?;

        let from = if self.match_token(TokenKind::From) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.match_token(TokenKind::Where) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let group_by = if self.match_token(TokenKind::Group) {
            self.expect(TokenKind::By)?;
            Some(self.parse_expr_list()?)
        } else {
            None
        };

        let having = if self.match_token(TokenKind::Having) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let window = if self.match_token(TokenKind::Window) {
            Some(self.parse_window_defs()?)
        } else {
            None
        };

        Ok(SelectCore {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            window,
        })
    }

    fn parse_values_core(&mut self) -> Result<SelectCore> {
        // VALUES was already consumed
        let mut rows = vec![self.parse_values_row()?];
        while self.match_token(TokenKind::Comma) {
            rows.push(self.parse_values_row()?);
        }

        // Convert VALUES to a SELECT with VALUES as a synthetic construct
        Ok(SelectCore {
            distinct: Distinct::All,
            columns: rows
                .into_iter()
                .next()
                .unwrap_or_default()
                .into_iter()
                .map(|e| ResultColumn::Expr {
                    expr: e,
                    alias: None,
                })
                .collect(),
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
        })
    }

    fn parse_values_row(&mut self) -> Result<Vec<Expr>> {
        self.expect(TokenKind::LParen)?;
        let exprs = self.parse_expr_list()?;
        self.expect(TokenKind::RParen)?;
        Ok(exprs)
    }

    fn parse_result_columns(&mut self) -> Result<Vec<ResultColumn>> {
        let mut columns = vec![self.parse_result_column()?];
        while self.match_token(TokenKind::Comma) {
            columns.push(self.parse_result_column()?);
        }
        Ok(columns)
    }

    fn parse_result_column(&mut self) -> Result<ResultColumn> {
        if self.match_token(TokenKind::Star) {
            return Ok(ResultColumn::Star);
        }

        // Check for table.*
        if self.check(TokenKind::Identifier) {
            let name = self.current_text().to_string();
            if self.peek().kind == TokenKind::Dot {
                self.advance(); // identifier
                self.advance(); // .
                if self.match_token(TokenKind::Star) {
                    return Ok(ResultColumn::TableStar(name));
                }
                // Not table.*, restore and parse as expression
                self.pos -= 2;
            }
        }

        let expr = self.parse_expr()?;
        let alias = if self.match_token(TokenKind::As) {
            Some(self.expect_identifier()?)
        } else if self.check(TokenKind::Identifier) && !self.check_keyword() {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(ResultColumn::Expr { expr, alias })
    }

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let mut tables = vec![self.parse_table_ref()?];

        // Handle implicit cross joins (comma-separated)
        while self.match_token(TokenKind::Comma) {
            tables.push(self.parse_table_ref()?);
        }

        Ok(FromClause { tables })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let mut table = self.parse_table_primary()?;

        // Handle JOINs
        loop {
            let join_type = self.parse_join_type();
            if join_type.is_none() {
                break;
            }
            let join_type = join_type.unwrap();

            self.expect(TokenKind::Join)?;
            let right = self.parse_table_primary()?;

            let constraint = if self.match_token(TokenKind::On) {
                Some(JoinConstraint::On(Box::new(self.parse_expr()?)))
            } else if self.match_token(TokenKind::Using) {
                self.expect(TokenKind::LParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(TokenKind::RParen)?;
                Some(JoinConstraint::Using(columns))
            } else {
                None
            };

            table = TableRef::Join {
                left: Box::new(table),
                join_type,
                right: Box::new(right),
                constraint,
            };
        }

        Ok(table)
    }

    fn parse_join_type(&mut self) -> Option<JoinType> {
        if self.match_token(TokenKind::Natural) {
            if self.match_token(TokenKind::Left) {
                self.match_token(TokenKind::Outer);
                return Some(JoinType::NaturalLeft);
            } else if self.match_token(TokenKind::Right) {
                self.match_token(TokenKind::Outer);
                return Some(JoinType::NaturalRight);
            } else if self.match_token(TokenKind::Full) {
                self.match_token(TokenKind::Outer);
                return Some(JoinType::NaturalFull);
            } else {
                return Some(JoinType::Natural);
            }
        }

        if self.match_token(TokenKind::Cross) {
            return Some(JoinType::Cross);
        }

        if self.match_token(TokenKind::Left) {
            self.match_token(TokenKind::Outer);
            return Some(JoinType::Left);
        }

        if self.match_token(TokenKind::Right) {
            self.match_token(TokenKind::Outer);
            return Some(JoinType::Right);
        }

        if self.match_token(TokenKind::Full) {
            self.match_token(TokenKind::Outer);
            return Some(JoinType::Full);
        }

        if self.match_token(TokenKind::Inner) {
            return Some(JoinType::Inner);
        }

        if self.check(TokenKind::Join) {
            return Some(JoinType::Inner);
        }

        None
    }

    fn parse_table_primary(&mut self) -> Result<TableRef> {
        if self.match_token(TokenKind::LParen) {
            if self.check(TokenKind::Select) || self.check(TokenKind::With) {
                let query = self.parse_select_stmt()?;
                self.expect(TokenKind::RParen)?;
                let alias = self.parse_table_alias()?;
                return Ok(TableRef::Subquery {
                    query: Box::new(query),
                    alias,
                });
            } else {
                let inner = self.parse_table_ref()?;
                self.expect(TokenKind::RParen)?;
                return Ok(TableRef::Parens(Box::new(inner)));
            }
        }

        // Table name or table function
        let name = self.parse_qualified_name()?;

        if self.match_token(TokenKind::LParen) {
            // Table-valued function
            let args = if self.check(TokenKind::RParen) {
                Vec::new()
            } else {
                self.parse_expr_list()?
            };
            self.expect(TokenKind::RParen)?;
            let alias = self.parse_table_alias()?;
            return Ok(TableRef::TableFunction {
                name: name.name,
                args,
                alias,
            });
        }

        let alias = self.parse_table_alias()?;

        let indexed_by = if self.match_token(TokenKind::Indexed) {
            self.expect(TokenKind::By)?;
            Some(IndexedBy::Index(self.expect_identifier()?))
        } else if self.match_token(TokenKind::Not) {
            self.expect(TokenKind::Indexed)?;
            Some(IndexedBy::NotIndexed)
        } else {
            None
        };

        Ok(TableRef::Table {
            name,
            alias,
            indexed_by,
        })
    }

    fn parse_table_alias(&mut self) -> Result<Option<String>> {
        if self.match_token(TokenKind::As) {
            return Ok(Some(self.expect_identifier()?));
        }

        if self.check(TokenKind::Identifier) && !self.check_keyword_for_alias() {
            return Ok(Some(self.expect_identifier()?));
        }

        Ok(None)
    }

    fn parse_ordering_terms(&mut self) -> Result<Vec<OrderingTerm>> {
        let mut terms = vec![self.parse_ordering_term()?];
        while self.match_token(TokenKind::Comma) {
            terms.push(self.parse_ordering_term()?);
        }
        Ok(terms)
    }

    fn parse_ordering_term(&mut self) -> Result<OrderingTerm> {
        let expr = self.parse_expr()?;

        let order = if self.match_token(TokenKind::Desc) {
            SortOrder::Desc
        } else {
            self.match_token(TokenKind::Asc);
            SortOrder::Asc
        };

        let nulls = if self.match_token(TokenKind::Nulls) {
            if self.match_token(TokenKind::First) {
                NullsOrder::First
            } else {
                self.expect(TokenKind::Last)?;
                NullsOrder::Last
            }
        } else {
            NullsOrder::Default
        };

        Ok(OrderingTerm { expr, order, nulls })
    }

    fn parse_limit_clause(&mut self) -> Result<LimitClause> {
        let limit = Box::new(self.parse_expr()?);

        let offset = if self.match_token(TokenKind::Offset) {
            Some(Box::new(self.parse_expr()?))
        } else if self.match_token(TokenKind::Comma) {
            // LIMIT offset, count syntax
            let count = self.parse_expr()?;
            return Ok(LimitClause {
                limit: Box::new(count),
                offset: Some(limit),
            });
        } else {
            None
        };

        Ok(LimitClause { limit, offset })
    }

    fn parse_window_defs(&mut self) -> Result<Vec<WindowDef>> {
        let mut defs = vec![self.parse_window_def()?];
        while self.match_token(TokenKind::Comma) {
            defs.push(self.parse_window_def()?);
        }
        Ok(defs)
    }

    fn parse_window_def(&mut self) -> Result<WindowDef> {
        let name = self.expect_identifier()?;
        self.expect(TokenKind::As)?;
        let spec = self.parse_window_spec()?;
        Ok(WindowDef { name, spec })
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec> {
        self.expect(TokenKind::LParen)?;

        let base = if self.check(TokenKind::Identifier)
            && !self.check(TokenKind::Partition)
            && !self.check(TokenKind::Order)
            && !self.check(TokenKind::Rows)
            && !self.check(TokenKind::Range)
            && !self.check(TokenKind::Groups)
        {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        let partition_by = if self.match_token(TokenKind::Partition) {
            self.expect(TokenKind::By)?;
            Some(self.parse_expr_list()?)
        } else {
            None
        };

        let order_by = if self.match_token(TokenKind::Order) {
            self.expect(TokenKind::By)?;
            Some(self.parse_ordering_terms()?)
        } else {
            None
        };

        let frame = self.parse_window_frame()?;

        self.expect(TokenKind::RParen)?;

        Ok(WindowSpec {
            base,
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_window_frame(&mut self) -> Result<Option<WindowFrame>> {
        let mode = if self.match_token(TokenKind::Rows) {
            WindowFrameMode::Rows
        } else if self.match_token(TokenKind::Range) {
            WindowFrameMode::Range
        } else if self.match_token(TokenKind::Groups) {
            WindowFrameMode::Groups
        } else {
            return Ok(None);
        };

        let (start, end) = if self.match_token(TokenKind::Between) {
            let start = self.parse_frame_bound()?;
            self.expect(TokenKind::And)?;
            let end = self.parse_frame_bound()?;
            (start, Some(end))
        } else {
            (self.parse_frame_bound()?, None)
        };

        let exclude = if self.match_token(TokenKind::Exclude) {
            if self.match_token(TokenKind::No) {
                self.expect(TokenKind::Others)?;
                WindowFrameExclude::NoOthers
            } else if self.match_token(TokenKind::Current) {
                self.expect(TokenKind::Row)?;
                WindowFrameExclude::CurrentRow
            } else if self.match_token(TokenKind::Group) {
                WindowFrameExclude::Group
            } else {
                self.expect(TokenKind::Ties)?;
                WindowFrameExclude::Ties
            }
        } else {
            WindowFrameExclude::NoOthers
        };

        Ok(Some(WindowFrame {
            mode,
            start,
            end,
            exclude,
        }))
    }

    fn parse_frame_bound(&mut self) -> Result<WindowFrameBound> {
        if self.match_token(TokenKind::Current) {
            self.expect(TokenKind::Row)?;
            return Ok(WindowFrameBound::CurrentRow);
        }

        if self.match_token(TokenKind::Unbounded) {
            if self.match_token(TokenKind::Preceding) {
                return Ok(WindowFrameBound::UnboundedPreceding);
            } else {
                self.expect(TokenKind::Following)?;
                return Ok(WindowFrameBound::UnboundedFollowing);
            }
        }

        let expr = Box::new(self.parse_expr()?);
        if self.match_token(TokenKind::Preceding) {
            Ok(WindowFrameBound::Preceding(expr))
        } else {
            self.expect(TokenKind::Following)?;
            Ok(WindowFrameBound::Following(expr))
        }
    }

    // ========================================================================
    // INSERT Statement
    // ========================================================================

    fn parse_insert_stmt(&mut self) -> Result<InsertStmt> {
        let or_action = if self.match_token(TokenKind::Replace) {
            Some(ConflictAction::Replace)
        } else {
            self.expect(TokenKind::Insert)?;
            if self.match_token(TokenKind::Or) {
                Some(self.parse_conflict_action()?)
            } else {
                None
            }
        };

        self.expect(TokenKind::Into)?;
        let table = self.parse_qualified_name()?;

        let alias = if self.match_token(TokenKind::As) {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        let columns = if self.match_token(TokenKind::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen)?;
            Some(cols)
        } else {
            None
        };

        let source = if self.match_token(TokenKind::Default) {
            self.expect(TokenKind::Values)?;
            InsertSource::DefaultValues
        } else if self.match_token(TokenKind::Values) {
            let mut rows = vec![self.parse_values_row()?];
            while self.match_token(TokenKind::Comma) {
                rows.push(self.parse_values_row()?);
            }
            InsertSource::Values(rows)
        } else {
            InsertSource::Select(Box::new(self.parse_select_stmt()?))
        };

        let on_conflict = if self.match_token(TokenKind::On) {
            self.expect(TokenKind::Conflict)?;
            Some(self.parse_on_conflict()?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(InsertStmt {
            with: None,
            or_action,
            table,
            alias,
            columns,
            source,
            on_conflict,
            returning,
        })
    }

    fn parse_on_conflict(&mut self) -> Result<OnConflict> {
        let target = if self.match_token(TokenKind::LParen) {
            let columns = self.parse_indexed_columns()?;
            self.expect(TokenKind::RParen)?;
            let where_clause = if self.match_token(TokenKind::Where) {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            Some(ConflictTarget {
                columns,
                where_clause,
            })
        } else {
            None
        };

        self.expect(TokenKind::Do)?;

        let action = if self.match_token(TokenKind::Nothing) {
            ConflictResolution::Nothing
        } else {
            self.expect(TokenKind::Update)?;
            self.expect(TokenKind::Set)?;
            let assignments = self.parse_assignments()?;
            let where_clause = if self.match_token(TokenKind::Where) {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            ConflictResolution::Update {
                assignments,
                where_clause,
            }
        };

        Ok(OnConflict { target, action })
    }

    // ========================================================================
    // UPDATE Statement
    // ========================================================================

    fn parse_update_stmt(&mut self) -> Result<UpdateStmt> {
        self.expect(TokenKind::Update)?;

        let or_action = if self.match_token(TokenKind::Or) {
            Some(self.parse_conflict_action()?)
        } else {
            None
        };

        let table = self.parse_qualified_name()?;

        let alias = if self.match_token(TokenKind::As) {
            Some(self.expect_identifier()?)
        } else if self.check(TokenKind::Identifier) && !self.check_keyword_for_alias() {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        let indexed_by = if self.match_token(TokenKind::Indexed) {
            self.expect(TokenKind::By)?;
            Some(IndexedBy::Index(self.expect_identifier()?))
        } else if self.match_token(TokenKind::Not) {
            self.expect(TokenKind::Indexed)?;
            Some(IndexedBy::NotIndexed)
        } else {
            None
        };

        self.expect(TokenKind::Set)?;
        let assignments = self.parse_assignments()?;

        let from = if self.match_token(TokenKind::From) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.match_token(TokenKind::Where) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let returning = self.parse_returning()?;

        let order_by = if self.match_token(TokenKind::Order) {
            self.expect(TokenKind::By)?;
            Some(self.parse_ordering_terms()?)
        } else {
            None
        };

        let limit = if self.match_token(TokenKind::Limit) {
            Some(self.parse_limit_clause()?)
        } else {
            None
        };

        Ok(UpdateStmt {
            with: None,
            or_action,
            table,
            alias,
            indexed_by,
            assignments,
            from,
            where_clause,
            returning,
            order_by,
            limit,
        })
    }

    fn parse_assignments(&mut self) -> Result<Vec<Assignment>> {
        let mut assignments = vec![self.parse_assignment()?];
        while self.match_token(TokenKind::Comma) {
            assignments.push(self.parse_assignment()?);
        }
        Ok(assignments)
    }

    fn parse_assignment(&mut self) -> Result<Assignment> {
        let columns = if self.match_token(TokenKind::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen)?;
            cols
        } else {
            vec![self.expect_identifier()?]
        };

        self.expect(TokenKind::Eq)?;
        let expr = self.parse_expr()?;

        Ok(Assignment { columns, expr })
    }

    // ========================================================================
    // DELETE Statement
    // ========================================================================

    fn parse_delete_stmt(&mut self) -> Result<DeleteStmt> {
        self.expect(TokenKind::Delete)?;
        self.expect(TokenKind::From)?;

        let table = self.parse_qualified_name()?;

        let alias = if self.match_token(TokenKind::As) {
            Some(self.expect_identifier()?)
        } else if self.check(TokenKind::Identifier) && !self.check_keyword_for_alias() {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        let indexed_by = if self.match_token(TokenKind::Indexed) {
            self.expect(TokenKind::By)?;
            Some(IndexedBy::Index(self.expect_identifier()?))
        } else if self.match_token(TokenKind::Not) {
            self.expect(TokenKind::Indexed)?;
            Some(IndexedBy::NotIndexed)
        } else {
            None
        };

        let where_clause = if self.match_token(TokenKind::Where) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let returning = self.parse_returning()?;

        let order_by = if self.match_token(TokenKind::Order) {
            self.expect(TokenKind::By)?;
            Some(self.parse_ordering_terms()?)
        } else {
            None
        };

        let limit = if self.match_token(TokenKind::Limit) {
            Some(self.parse_limit_clause()?)
        } else {
            None
        };

        Ok(DeleteStmt {
            with: None,
            table,
            alias,
            indexed_by,
            where_clause,
            returning,
            order_by,
            limit,
        })
    }

    fn parse_returning(&mut self) -> Result<Option<Vec<ResultColumn>>> {
        if self.match_token(TokenKind::Returning) {
            Ok(Some(self.parse_result_columns()?))
        } else {
            Ok(None)
        }
    }

    // ========================================================================
    // CREATE Statements
    // ========================================================================

    fn parse_create(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Create)?;

        let temporary = self.match_token(TokenKind::Temp) || self.match_token(TokenKind::Temporary);
        let unique = self.match_token(TokenKind::Unique);

        if self.match_token(TokenKind::Table) {
            return Ok(Stmt::CreateTable(self.parse_create_table(temporary)?));
        }

        if self.match_token(TokenKind::Index) {
            return Ok(Stmt::CreateIndex(self.parse_create_index(unique)?));
        }

        if self.match_token(TokenKind::View) {
            return Ok(Stmt::CreateView(self.parse_create_view(temporary)?));
        }

        if self.match_token(TokenKind::Trigger) {
            return Ok(Stmt::CreateTrigger(self.parse_create_trigger(temporary)?));
        }

        Err(self.error("expected TABLE, INDEX, VIEW, or TRIGGER after CREATE"))
    }

    fn parse_create_table(&mut self, temporary: bool) -> Result<CreateTableStmt> {
        let if_not_exists = if self.match_token(TokenKind::If) {
            self.expect(TokenKind::Not)?;
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_qualified_name()?;

        let definition = if self.match_token(TokenKind::As) {
            TableDefinition::AsSelect(Box::new(self.parse_select_stmt()?))
        } else {
            self.expect(TokenKind::LParen)?;
            let columns = self.parse_column_defs()?;
            let constraints = self.parse_table_constraints()?;
            self.expect(TokenKind::RParen)?;
            TableDefinition::Columns {
                columns,
                constraints,
            }
        };

        let mut without_rowid = false;
        let mut strict = false;

        while self.check(TokenKind::Without) || self.check(TokenKind::Identifier) {
            if self.match_token(TokenKind::Without) {
                // WITHOUT ROWID
                self.expect_keyword("ROWID")?;
                without_rowid = true;
            } else if self.current_text().eq_ignore_ascii_case("STRICT") {
                self.advance();
                strict = true;
            } else {
                break;
            }

            self.match_token(TokenKind::Comma);
        }

        Ok(CreateTableStmt {
            temporary,
            if_not_exists,
            name,
            definition,
            without_rowid,
            strict,
        })
    }

    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::new();

        loop {
            // Check if this looks like a column definition
            if !self.check(TokenKind::Identifier) && !self.check(TokenKind::String) {
                break;
            }

            // Peek to see if the next token is a constraint keyword (table constraint)
            if self.is_table_constraint_start() {
                break;
            }

            columns.push(self.parse_column_def()?);

            if !self.match_token(TokenKind::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    fn is_table_constraint_start(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::Constraint
                | TokenKind::Primary
                | TokenKind::Unique
                | TokenKind::Check
                | TokenKind::Foreign
        )
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.expect_identifier()?;

        let type_name = if self.check(TokenKind::Identifier) && !self.is_column_constraint_start() {
            Some(self.parse_type_name()?)
        } else {
            None
        };

        let constraints = self.parse_column_constraints()?;

        Ok(ColumnDef {
            name,
            type_name,
            constraints,
        })
    }

    fn is_column_constraint_start(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::Constraint
                | TokenKind::Primary
                | TokenKind::Not
                | TokenKind::Unique
                | TokenKind::Check
                | TokenKind::Default
                | TokenKind::Collate
                | TokenKind::References
                | TokenKind::Generated
                | TokenKind::As
        )
    }

    fn parse_type_name(&mut self) -> Result<TypeName> {
        let mut name = self.expect_identifier()?;

        // Handle multi-word type names like "VARYING CHARACTER"
        while self.check(TokenKind::Identifier) && !self.is_column_constraint_start() {
            name.push(' ');
            name.push_str(&self.expect_identifier()?);
        }

        let args = if self.match_token(TokenKind::LParen) {
            let mut args = Vec::new();
            if self.check(TokenKind::Integer) || self.check(TokenKind::Minus) {
                let neg = self.match_token(TokenKind::Minus);
                let val: i64 = self.expect_integer()?;
                args.push(if neg { -val } else { val });

                if self.match_token(TokenKind::Comma) {
                    let neg = self.match_token(TokenKind::Minus);
                    let val: i64 = self.expect_integer()?;
                    args.push(if neg { -val } else { val });
                }
            }
            self.expect(TokenKind::RParen)?;
            args
        } else {
            Vec::new()
        };

        Ok(TypeName { name, args })
    }

    fn parse_column_constraints(&mut self) -> Result<Vec<ColumnConstraint>> {
        let mut constraints = Vec::new();

        loop {
            let name = if self.match_token(TokenKind::Constraint) {
                Some(self.expect_identifier()?)
            } else {
                None
            };

            let kind = if self.match_token(TokenKind::Primary) {
                self.expect(TokenKind::Key)?;
                let order = self.parse_sort_order();
                let conflict = self.parse_conflict_clause()?;
                let autoincrement = self.match_token(TokenKind::Autoincrement);
                ColumnConstraintKind::PrimaryKey {
                    order,
                    conflict,
                    autoincrement,
                }
            } else if self.match_token(TokenKind::Not) {
                self.expect(TokenKind::Null)?;
                let conflict = self.parse_conflict_clause()?;
                ColumnConstraintKind::NotNull { conflict }
            } else if self.match_token(TokenKind::Unique) {
                let conflict = self.parse_conflict_clause()?;
                ColumnConstraintKind::Unique { conflict }
            } else if self.match_token(TokenKind::Check) {
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                ColumnConstraintKind::Check(Box::new(expr))
            } else if self.match_token(TokenKind::Default) {
                ColumnConstraintKind::Default(self.parse_default_value()?)
            } else if self.match_token(TokenKind::Collate) {
                ColumnConstraintKind::Collate(self.expect_identifier()?)
            } else if self.match_token(TokenKind::References) {
                ColumnConstraintKind::ForeignKey(self.parse_foreign_key_clause()?)
            } else if self.match_token(TokenKind::Generated) || self.match_token(TokenKind::As) {
                // GENERATED ALWAYS AS or AS
                if self.current().kind == TokenKind::Always {
                    self.advance();
                    self.expect(TokenKind::As)?;
                }
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                let storage = if self.match_token(TokenKind::Stored) {
                    GeneratedStorage::Stored
                } else {
                    self.match_token(TokenKind::Virtual);
                    GeneratedStorage::Virtual
                };
                ColumnConstraintKind::Generated {
                    expr: Box::new(expr),
                    storage,
                }
            } else if name.is_some() {
                return Err(self.error("expected constraint after CONSTRAINT"));
            } else {
                break;
            };

            constraints.push(ColumnConstraint { name, kind });
        }

        Ok(constraints)
    }

    fn parse_default_value(&mut self) -> Result<DefaultValue> {
        if self.match_token(TokenKind::LParen) {
            let expr = self.parse_expr()?;
            self.expect(TokenKind::RParen)?;
            return Ok(DefaultValue::Expr(Box::new(expr)));
        }

        if self.match_token(TokenKind::CurrentTime) {
            return Ok(DefaultValue::CurrentTime);
        }

        if self.match_token(TokenKind::CurrentDate) {
            return Ok(DefaultValue::CurrentDate);
        }

        if self.match_token(TokenKind::CurrentTimestamp) {
            return Ok(DefaultValue::CurrentTimestamp);
        }

        // Literal value
        let literal = self.parse_literal()?;
        Ok(DefaultValue::Literal(literal))
    }

    fn parse_foreign_key_clause(&mut self) -> Result<ForeignKeyClause> {
        let table = self.expect_identifier()?;

        let columns = if self.match_token(TokenKind::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen)?;
            Some(cols)
        } else {
            None
        };

        let mut on_delete = None;
        let mut on_update = None;
        let mut match_type = None;

        loop {
            if self.match_token(TokenKind::On) {
                if self.match_token(TokenKind::Delete) {
                    on_delete = Some(self.parse_foreign_key_action()?);
                } else {
                    self.expect(TokenKind::Update)?;
                    on_update = Some(self.parse_foreign_key_action()?);
                }
            } else if self.match_token(TokenKind::Match) {
                match_type = Some(self.expect_identifier()?);
            } else {
                break;
            }
        }

        let deferrable = self.parse_deferrable()?;

        Ok(ForeignKeyClause {
            table,
            columns,
            on_delete,
            on_update,
            match_type,
            deferrable,
        })
    }

    fn parse_foreign_key_action(&mut self) -> Result<ForeignKeyAction> {
        if self.match_token(TokenKind::Set) {
            if self.match_token(TokenKind::Null) {
                Ok(ForeignKeyAction::SetNull)
            } else {
                self.expect(TokenKind::Default)?;
                Ok(ForeignKeyAction::SetDefault)
            }
        } else if self.match_token(TokenKind::Cascade) {
            Ok(ForeignKeyAction::Cascade)
        } else if self.match_token(TokenKind::Restrict) {
            Ok(ForeignKeyAction::Restrict)
        } else if self.match_token(TokenKind::No) {
            self.expect(TokenKind::Action)?;
            Ok(ForeignKeyAction::NoAction)
        } else {
            Err(self.error("expected SET NULL, SET DEFAULT, CASCADE, RESTRICT, or NO ACTION"))
        }
    }

    fn parse_deferrable(&mut self) -> Result<Option<Deferrable>> {
        let not = self.match_token(TokenKind::Not);
        if !self.match_token(TokenKind::Deferrable) {
            if not {
                return Err(self.error("expected DEFERRABLE after NOT"));
            }
            return Ok(None);
        }

        let initially = if self.match_token(TokenKind::Initially) {
            if self.match_token(TokenKind::Deferred) {
                Some(DeferrableInitially::Deferred)
            } else {
                self.expect(TokenKind::Immediate)?;
                Some(DeferrableInitially::Immediate)
            }
        } else {
            None
        };

        Ok(Some(Deferrable { not, initially }))
    }

    fn parse_table_constraints(&mut self) -> Result<Vec<TableConstraint>> {
        let mut constraints = Vec::new();

        while self.is_table_constraint_start() {
            let name = if self.match_token(TokenKind::Constraint) {
                Some(self.expect_identifier()?)
            } else {
                None
            };

            let kind = if self.match_token(TokenKind::Primary) {
                self.expect(TokenKind::Key)?;
                self.expect(TokenKind::LParen)?;
                let columns = self.parse_indexed_columns()?;
                self.expect(TokenKind::RParen)?;
                let conflict = self.parse_conflict_clause()?;
                TableConstraintKind::PrimaryKey { columns, conflict }
            } else if self.match_token(TokenKind::Unique) {
                self.expect(TokenKind::LParen)?;
                let columns = self.parse_indexed_columns()?;
                self.expect(TokenKind::RParen)?;
                let conflict = self.parse_conflict_clause()?;
                TableConstraintKind::Unique { columns, conflict }
            } else if self.match_token(TokenKind::Check) {
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                TableConstraintKind::Check(Box::new(expr))
            } else if self.match_token(TokenKind::Foreign) {
                self.expect(TokenKind::Key)?;
                self.expect(TokenKind::LParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(TokenKind::RParen)?;
                self.expect(TokenKind::References)?;
                let clause = self.parse_foreign_key_clause()?;
                TableConstraintKind::ForeignKey { columns, clause }
            } else {
                break;
            };

            constraints.push(TableConstraint { name, kind });

            self.match_token(TokenKind::Comma);
        }

        Ok(constraints)
    }

    fn parse_indexed_columns(&mut self) -> Result<Vec<IndexedColumn>> {
        let mut columns = vec![self.parse_indexed_column()?];
        while self.match_token(TokenKind::Comma) {
            columns.push(self.parse_indexed_column()?);
        }
        Ok(columns)
    }

    fn parse_indexed_column(&mut self) -> Result<IndexedColumn> {
        let column = if self.match_token(TokenKind::LParen) {
            let expr = self.parse_expr()?;
            self.expect(TokenKind::RParen)?;
            IndexedColumnKind::Expr(Box::new(expr))
        } else {
            IndexedColumnKind::Name(self.expect_identifier()?)
        };

        let collation = if self.match_token(TokenKind::Collate) {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        let order = self.parse_sort_order();

        Ok(IndexedColumn {
            column,
            collation,
            order,
        })
    }

    fn parse_sort_order(&mut self) -> Option<SortOrder> {
        if self.match_token(TokenKind::Asc) {
            Some(SortOrder::Asc)
        } else if self.match_token(TokenKind::Desc) {
            Some(SortOrder::Desc)
        } else {
            None
        }
    }

    fn parse_conflict_clause(&mut self) -> Result<Option<ConflictAction>> {
        if self.match_token(TokenKind::On) {
            self.expect(TokenKind::Conflict)?;
            Ok(Some(self.parse_conflict_action()?))
        } else {
            Ok(None)
        }
    }

    fn parse_conflict_action(&mut self) -> Result<ConflictAction> {
        if self.match_token(TokenKind::Rollback) {
            Ok(ConflictAction::Rollback)
        } else if self.match_token(TokenKind::Abort) {
            Ok(ConflictAction::Abort)
        } else if self.match_token(TokenKind::Fail) {
            Ok(ConflictAction::Fail)
        } else if self.match_token(TokenKind::Ignore) {
            Ok(ConflictAction::Ignore)
        } else if self.match_token(TokenKind::Replace) {
            Ok(ConflictAction::Replace)
        } else {
            Err(self.error("expected ROLLBACK, ABORT, FAIL, IGNORE, or REPLACE"))
        }
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<CreateIndexStmt> {
        let if_not_exists = if self.match_token(TokenKind::If) {
            self.expect(TokenKind::Not)?;
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_qualified_name()?;
        self.expect(TokenKind::On)?;
        let table = self.expect_identifier()?;

        self.expect(TokenKind::LParen)?;
        let columns = self.parse_indexed_columns()?;
        self.expect(TokenKind::RParen)?;

        let where_clause = if self.match_token(TokenKind::Where) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        Ok(CreateIndexStmt {
            unique,
            if_not_exists,
            name,
            table,
            columns,
            where_clause,
        })
    }

    fn parse_create_view(&mut self, temporary: bool) -> Result<CreateViewStmt> {
        let if_not_exists = if self.match_token(TokenKind::If) {
            self.expect(TokenKind::Not)?;
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_qualified_name()?;

        let columns = if self.match_token(TokenKind::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(TokenKind::As)?;
        let query = Box::new(self.parse_select_stmt()?);

        Ok(CreateViewStmt {
            temporary,
            if_not_exists,
            name,
            columns,
            query,
        })
    }

    fn parse_create_trigger(&mut self, temporary: bool) -> Result<CreateTriggerStmt> {
        let if_not_exists = if self.match_token(TokenKind::If) {
            self.expect(TokenKind::Not)?;
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_qualified_name()?;

        let time = if self.match_token(TokenKind::Before) {
            TriggerTime::Before
        } else if self.match_token(TokenKind::After) {
            TriggerTime::After
        } else if self.match_token(TokenKind::Instead) {
            self.expect(TokenKind::Of)?;
            TriggerTime::InsteadOf
        } else {
            return Err(self.error("expected BEFORE, AFTER, or INSTEAD OF"));
        };

        let event = if self.match_token(TokenKind::Delete) {
            TriggerEvent::Delete
        } else if self.match_token(TokenKind::Insert) {
            TriggerEvent::Insert
        } else {
            self.expect(TokenKind::Update)?;
            let columns = if self.match_token(TokenKind::Of) {
                Some(self.parse_identifier_list()?)
            } else {
                None
            };
            TriggerEvent::Update(columns)
        };

        self.expect(TokenKind::On)?;
        let table = self.expect_identifier()?;

        let for_each_row = if self.match_token(TokenKind::For) {
            self.expect(TokenKind::Each)?;
            self.expect(TokenKind::Row)?;
            true
        } else {
            false
        };

        let when = if self.match_token(TokenKind::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(TokenKind::Begin)?;
        let mut body = Vec::new();
        while !self.check(TokenKind::End) {
            body.push(self.parse_stmt()?);
            self.match_token(TokenKind::Semicolon);
        }
        self.expect(TokenKind::End)?;

        Ok(CreateTriggerStmt {
            temporary,
            if_not_exists,
            name,
            time,
            event,
            table,
            for_each_row,
            when,
            body,
        })
    }

    // ========================================================================
    // DROP Statements
    // ========================================================================

    fn parse_drop(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Drop)?;

        if self.match_token(TokenKind::Table) {
            return Ok(Stmt::DropTable(self.parse_drop_stmt()?));
        }

        if self.match_token(TokenKind::Index) {
            return Ok(Stmt::DropIndex(self.parse_drop_stmt()?));
        }

        if self.match_token(TokenKind::View) {
            return Ok(Stmt::DropView(self.parse_drop_stmt()?));
        }

        if self.match_token(TokenKind::Trigger) {
            return Ok(Stmt::DropTrigger(self.parse_drop_stmt()?));
        }

        Err(self.error("expected TABLE, INDEX, VIEW, or TRIGGER after DROP"))
    }

    fn parse_drop_stmt(&mut self) -> Result<DropStmt> {
        let if_exists = if self.match_token(TokenKind::If) {
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_qualified_name()?;

        Ok(DropStmt { if_exists, name })
    }

    // ========================================================================
    // ALTER Statement
    // ========================================================================

    fn parse_alter(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Alter)?;
        self.expect(TokenKind::Table)?;

        let table = self.parse_qualified_name()?;

        let action = if self.match_token(TokenKind::Rename) {
            if self.match_token(TokenKind::To) {
                AlterTableAction::RenameTable(self.expect_identifier()?)
            } else if self.match_token(TokenKind::Column) {
                let old = self.expect_identifier()?;
                self.expect(TokenKind::To)?;
                let new = self.expect_identifier()?;
                AlterTableAction::RenameColumn { old, new }
            } else {
                let old = self.expect_identifier()?;
                self.expect(TokenKind::To)?;
                let new = self.expect_identifier()?;
                AlterTableAction::RenameColumn { old, new }
            }
        } else if self.match_token(TokenKind::Add) {
            self.match_token(TokenKind::Column);
            AlterTableAction::AddColumn(self.parse_column_def()?)
        } else if self.match_token(TokenKind::Drop) {
            self.match_token(TokenKind::Column);
            AlterTableAction::DropColumn(self.expect_identifier()?)
        } else {
            return Err(self.error("expected RENAME, ADD, or DROP"));
        };

        Ok(Stmt::AlterTable(AlterTableStmt { table, action }))
    }

    // ========================================================================
    // Transaction Statements
    // ========================================================================

    fn parse_begin(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Begin)?;

        let mode = if self.match_token(TokenKind::Deferred) {
            Some(TransactionMode::Deferred)
        } else if self.match_token(TokenKind::Immediate) {
            Some(TransactionMode::Immediate)
        } else if self.match_token(TokenKind::Exclusive) {
            Some(TransactionMode::Exclusive)
        } else {
            None
        };

        self.match_token(TokenKind::Transaction);

        Ok(Stmt::Begin(BeginStmt { mode }))
    }

    fn parse_rollback(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Rollback)?;
        self.match_token(TokenKind::Transaction);

        let savepoint = if self.match_token(TokenKind::To) {
            self.match_token(TokenKind::Savepoint);
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(Stmt::Rollback(RollbackStmt { savepoint }))
    }

    // ========================================================================
    // Other Statements
    // ========================================================================

    fn parse_pragma(&mut self) -> Result<PragmaStmt> {
        self.expect(TokenKind::Pragma)?;

        let (schema, name) = self.parse_maybe_qualified_name()?;

        let value = if self.match_token(TokenKind::Eq) {
            Some(PragmaValue::Set(self.parse_expr()?))
        } else if self.match_token(TokenKind::LParen) {
            let expr = self.parse_expr()?;
            self.expect(TokenKind::RParen)?;
            Some(PragmaValue::Call(expr))
        } else {
            None
        };

        Ok(PragmaStmt {
            schema,
            name,
            value,
        })
    }

    fn parse_vacuum(&mut self) -> Result<VacuumStmt> {
        self.expect(TokenKind::Vacuum)?;

        let schema = if self.check(TokenKind::Identifier) && !self.check(TokenKind::Into) {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        let into = if self.match_token(TokenKind::Into) {
            Some(self.expect_string()?)
        } else {
            None
        };

        Ok(VacuumStmt { schema, into })
    }

    fn parse_analyze(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Analyze)?;

        if self.check(TokenKind::Identifier) {
            Ok(Stmt::Analyze(Some(self.parse_qualified_name()?)))
        } else {
            Ok(Stmt::Analyze(None))
        }
    }

    fn parse_reindex(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Reindex)?;

        if self.check(TokenKind::Identifier) {
            Ok(Stmt::Reindex(Some(self.parse_qualified_name()?)))
        } else {
            Ok(Stmt::Reindex(None))
        }
    }

    fn parse_attach(&mut self) -> Result<AttachStmt> {
        self.expect(TokenKind::Attach)?;
        self.match_token(TokenKind::Database);

        let expr = self.parse_expr()?;
        self.expect(TokenKind::As)?;
        let schema = self.expect_identifier()?;

        Ok(AttachStmt { expr, schema })
    }

    // ========================================================================
    // Expression Parser
    // ========================================================================

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while self.match_token(TokenKind::Or) {
            let right = self.parse_and_expr()?;
            left = Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expr()?;

        while self.match_token(TokenKind::And) {
            let right = self.parse_not_expr()?;
            left = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr> {
        if self.match_token(TokenKind::Not) {
            let expr = self.parse_not_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }

        self.parse_comparison_expr()
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_bitwise_or_expr()?;

        // Handle comparison operators and special forms
        loop {
            if self.match_token(TokenKind::Eq) || self.match_token(TokenKind::EqEq) {
                let right = self.parse_bitwise_or_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Ne) || self.match_token(TokenKind::BangEq) {
                let right = self.parse_bitwise_or_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Ne,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Lt) {
                let right = self.parse_bitwise_or_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Lt,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Le) {
                let right = self.parse_bitwise_or_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Le,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Gt) {
                let right = self.parse_bitwise_or_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Ge) {
                let right = self.parse_bitwise_or_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Ge,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Is) {
                let negated = self.match_token(TokenKind::Not);
                if self.match_token(TokenKind::Distinct) {
                    self.expect(TokenKind::From)?;
                    let right = self.parse_bitwise_or_expr()?;
                    left = Expr::IsDistinct {
                        left: Box::new(left),
                        right: Box::new(right),
                        negated,
                    };
                } else if self.check(TokenKind::Null) {
                    self.advance();
                    left = Expr::IsNull {
                        expr: Box::new(left),
                        negated,
                    };
                } else {
                    let right = self.parse_bitwise_or_expr()?;
                    left = Expr::Binary {
                        op: if negated {
                            BinaryOp::IsNot
                        } else {
                            BinaryOp::Is
                        },
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
            } else if self.match_token(TokenKind::Isnull) {
                left = Expr::IsNull {
                    expr: Box::new(left),
                    negated: false,
                };
            } else if self.match_token(TokenKind::Notnull) {
                left = Expr::IsNull {
                    expr: Box::new(left),
                    negated: true,
                };
            } else if self.match_token(TokenKind::Not) {
                left = self.parse_not_suffix(left)?;
            } else if self.match_token(TokenKind::Between) {
                let low = self.parse_bitwise_or_expr()?;
                self.expect(TokenKind::And)?;
                let high = self.parse_bitwise_or_expr()?;
                left = Expr::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: false,
                };
            } else if self.match_token(TokenKind::In) {
                left = self.parse_in_expr(left, false)?;
            } else if self.match_token(TokenKind::Like) {
                left = self.parse_like_expr(left, LikeOp::Like, false)?;
            } else if self.match_token(TokenKind::Glob) {
                left = self.parse_like_expr(left, LikeOp::Glob, false)?;
            } else if self.match_token(TokenKind::Regexp) {
                left = self.parse_like_expr(left, LikeOp::Regexp, false)?;
            } else if self.match_token(TokenKind::Match) {
                left = self.parse_like_expr(left, LikeOp::Match, false)?;
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_not_suffix(&mut self, left: Expr) -> Result<Expr> {
        if self.match_token(TokenKind::Between) {
            let low = self.parse_bitwise_or_expr()?;
            self.expect(TokenKind::And)?;
            let high = self.parse_bitwise_or_expr()?;
            Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: true,
            })
        } else if self.match_token(TokenKind::In) {
            self.parse_in_expr(left, true)
        } else if self.match_token(TokenKind::Like) {
            self.parse_like_expr(left, LikeOp::Like, true)
        } else if self.match_token(TokenKind::Glob) {
            self.parse_like_expr(left, LikeOp::Glob, true)
        } else if self.match_token(TokenKind::Regexp) {
            self.parse_like_expr(left, LikeOp::Regexp, true)
        } else if self.match_token(TokenKind::Match) {
            self.parse_like_expr(left, LikeOp::Match, true)
        } else if self.match_token(TokenKind::Null) {
            Ok(Expr::IsNull {
                expr: Box::new(left),
                negated: true,
            })
        } else {
            Err(self.error("expected BETWEEN, IN, LIKE, GLOB, REGEXP, MATCH, or NULL after NOT"))
        }
    }

    fn parse_in_expr(&mut self, left: Expr, negated: bool) -> Result<Expr> {
        self.expect(TokenKind::LParen)?;

        let list = if self.check(TokenKind::Select) || self.check(TokenKind::With) {
            InList::Subquery(Box::new(self.parse_select_stmt()?))
        } else if self.check(TokenKind::RParen) {
            InList::Values(Vec::new())
        } else {
            InList::Values(self.parse_expr_list()?)
        };

        self.expect(TokenKind::RParen)?;

        Ok(Expr::In {
            expr: Box::new(left),
            list,
            negated,
        })
    }

    fn parse_like_expr(&mut self, left: Expr, op: LikeOp, negated: bool) -> Result<Expr> {
        let pattern = self.parse_bitwise_or_expr()?;

        let escape = if self.match_token(TokenKind::Escape) {
            Some(Box::new(self.parse_bitwise_or_expr()?))
        } else {
            None
        };

        Ok(Expr::Like {
            expr: Box::new(left),
            pattern: Box::new(pattern),
            escape,
            op,
            negated,
        })
    }

    fn parse_bitwise_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_bitwise_and_expr()?;

        while self.match_token(TokenKind::Pipe) {
            let right = self.parse_bitwise_and_expr()?;
            left = Expr::Binary {
                op: BinaryOp::BitOr,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_bitwise_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_shift_expr()?;

        while self.match_token(TokenKind::Ampersand) {
            let right = self.parse_shift_expr()?;
            left = Expr::Binary {
                op: BinaryOp::BitAnd,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_shift_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_additive_expr()?;

        loop {
            if self.match_token(TokenKind::LtLt) {
                let right = self.parse_additive_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::ShiftLeft,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::GtGt) {
                let right = self.parse_additive_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::ShiftRight,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_additive_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            if self.match_token(TokenKind::Plus) {
                let right = self.parse_multiplicative_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Minus) {
                let right = self.parse_multiplicative_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Sub,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_concat_expr()?;

        loop {
            if self.match_token(TokenKind::Star) {
                let right = self.parse_concat_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Slash) {
                let right = self.parse_concat_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Div,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if self.match_token(TokenKind::Percent) {
                let right = self.parse_concat_expr()?;
                left = Expr::Binary {
                    op: BinaryOp::Mod,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_concat_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;

        while self.match_token(TokenKind::DoublePipe) {
            let right = self.parse_unary_expr()?;
            left = Expr::Binary {
                op: BinaryOp::Concat,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr> {
        if self.match_token(TokenKind::Minus) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }

        if self.match_token(TokenKind::Plus) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Pos,
                expr: Box::new(expr),
            });
        }

        if self.match_token(TokenKind::Tilde) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::BitNot,
                expr: Box::new(expr),
            });
        }

        self.parse_collate_expr()
    }

    fn parse_collate_expr(&mut self) -> Result<Expr> {
        let mut expr = self.parse_primary_expr()?;

        if self.match_token(TokenKind::Collate) {
            let collation = self.expect_identifier()?;
            expr = Expr::Collate {
                expr: Box::new(expr),
                collation,
            };
        }

        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr> {
        // Literals
        if self.check(TokenKind::Integer)
            || self.check(TokenKind::Float)
            || self.check(TokenKind::String)
            || self.check(TokenKind::Blob)
            || self.check(TokenKind::Null)
            || self.check(TokenKind::CurrentTime)
            || self.check(TokenKind::CurrentDate)
            || self.check(TokenKind::CurrentTimestamp)
        {
            return Ok(Expr::Literal(self.parse_literal()?));
        }

        // Parameters (variables like ?, ?1, :name, @name, $name)
        if self.check(TokenKind::Variable) {
            return Ok(Expr::Variable(self.parse_variable()?));
        }

        // CASE expression
        if self.match_token(TokenKind::Case) {
            return self.parse_case_expr();
        }

        // CAST expression
        if self.match_token(TokenKind::Cast) {
            return self.parse_cast_expr();
        }

        // EXISTS subquery
        if self.match_token(TokenKind::Exists) {
            self.expect(TokenKind::LParen)?;
            let subquery = self.parse_select_stmt()?;
            self.expect(TokenKind::RParen)?;
            return Ok(Expr::Exists {
                subquery: Box::new(subquery),
                negated: false,
            });
        }

        // NOT EXISTS subquery
        if self.match_token(TokenKind::Not) {
            if self.match_token(TokenKind::Exists) {
                self.expect(TokenKind::LParen)?;
                let subquery = self.parse_select_stmt()?;
                self.expect(TokenKind::RParen)?;
                return Ok(Expr::Exists {
                    subquery: Box::new(subquery),
                    negated: true,
                });
            }
            // NOT expression
            let expr = self.parse_primary_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }

        // RAISE function
        if self.match_token(TokenKind::Raise) {
            return self.parse_raise_expr();
        }

        // Parenthesized expression or subquery
        if self.match_token(TokenKind::LParen) {
            if self.check(TokenKind::Select) || self.check(TokenKind::With) {
                let subquery = self.parse_select_stmt()?;
                self.expect(TokenKind::RParen)?;
                return Ok(Expr::Subquery(Box::new(subquery)));
            }
            let expr = self.parse_expr()?;
            self.expect(TokenKind::RParen)?;
            return Ok(Expr::Parens(Box::new(expr)));
        }

        // Identifier (column reference or function call)
        if self.check(TokenKind::Identifier) {
            return self.parse_identifier_or_function();
        }

        Err(self.error("expected expression"))
    }

    fn parse_literal(&mut self) -> Result<Literal> {
        let token = self.current().clone();
        self.advance();

        match token.kind {
            TokenKind::Null => Ok(Literal::Null),
            TokenKind::Integer => {
                let text = token.text(self.source);
                let value = if text.starts_with("0x") || text.starts_with("0X") {
                    i64::from_str_radix(&text[2..], 16)
                        .map_err(|_| Error::with_message(ErrorCode::Error, "invalid hex integer"))?
                } else {
                    text.parse()
                        .map_err(|_| Error::with_message(ErrorCode::Error, "invalid integer"))?
                };
                Ok(Literal::Integer(value))
            }
            TokenKind::Float => {
                let value = token
                    .text(self.source)
                    .parse()
                    .map_err(|_| Error::with_message(ErrorCode::Error, "invalid float"))?;
                Ok(Literal::Float(value))
            }
            TokenKind::String => {
                let text = token.text(self.source);
                // Remove quotes and unescape
                let inner = &text[1..text.len() - 1];
                let value = inner.replace("''", "'");
                Ok(Literal::String(value))
            }
            TokenKind::Blob => {
                let text = token.text(self.source);
                // X'...' or x'...'
                let hex = &text[2..text.len() - 1];
                let bytes = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                    .collect::<std::result::Result<Vec<u8>, _>>()
                    .map_err(|_| Error::with_message(ErrorCode::Error, "invalid blob literal"))?;
                Ok(Literal::Blob(bytes))
            }
            TokenKind::CurrentTime => Ok(Literal::CurrentTime),
            TokenKind::CurrentDate => Ok(Literal::CurrentDate),
            TokenKind::CurrentTimestamp => Ok(Literal::CurrentTimestamp),
            _ => Err(self.error("expected literal")),
        }
    }

    fn parse_variable(&mut self) -> Result<Variable> {
        // Variables are now returned as single tokens by the tokenizer
        let text = self.current_text().to_string();
        self.advance();

        if let Some(num_part) = text.strip_prefix('?') {
            // Numbered parameter: ? or ?NNN
            if num_part.is_empty() {
                return Ok(Variable::Numbered(None));
            }
            let num: i32 = num_part
                .parse()
                .map_err(|_| Error::with_message(ErrorCode::Error, "invalid parameter number"))?;
            return Ok(Variable::Numbered(Some(num)));
        }

        // Named parameter: :name, @name, $name
        let prefix = text.chars().next().unwrap();
        let name = text[1..].to_string();

        Ok(Variable::Named { prefix, name })
    }

    fn parse_case_expr(&mut self) -> Result<Expr> {
        let operand = if !self.check(TokenKind::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.match_token(TokenKind::When) {
            let when = self.parse_expr()?;
            self.expect(TokenKind::Then)?;
            let then = self.parse_expr()?;
            when_clauses.push(WhenClause {
                when: Box::new(when),
                then: Box::new(then),
            });
        }

        let else_clause = if self.match_token(TokenKind::Else) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(TokenKind::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn parse_cast_expr(&mut self) -> Result<Expr> {
        self.expect(TokenKind::LParen)?;
        let expr = self.parse_expr()?;
        self.expect(TokenKind::As)?;
        let type_name = self.parse_type_name()?;
        self.expect(TokenKind::RParen)?;

        Ok(Expr::Cast {
            expr: Box::new(expr),
            type_name,
        })
    }

    fn parse_raise_expr(&mut self) -> Result<Expr> {
        self.expect(TokenKind::LParen)?;

        let action = if self.match_token(TokenKind::Ignore) {
            RaiseAction::Ignore
        } else if self.match_token(TokenKind::Rollback) {
            RaiseAction::Rollback
        } else if self.match_token(TokenKind::Abort) {
            RaiseAction::Abort
        } else if self.match_token(TokenKind::Fail) {
            RaiseAction::Fail
        } else {
            return Err(self.error("expected IGNORE, ROLLBACK, ABORT, or FAIL"));
        };

        let message = if action != RaiseAction::Ignore {
            self.expect(TokenKind::Comma)?;
            Some(self.expect_string()?)
        } else {
            None
        };

        self.expect(TokenKind::RParen)?;

        Ok(Expr::Raise { action, message })
    }

    fn parse_identifier_or_function(&mut self) -> Result<Expr> {
        let first = self.expect_identifier()?;

        // Check for function call
        if self.match_token(TokenKind::LParen) {
            return self.parse_function_call(first);
        }

        // Check for qualified name
        if self.match_token(TokenKind::Dot) {
            let second = self.expect_identifier()?;
            if self.match_token(TokenKind::Dot) {
                // db.table.column
                let column = self.expect_identifier()?;
                return Ok(Expr::Column(ColumnRef {
                    database: Some(first),
                    table: Some(second),
                    column,
                    column_index: None,
                }));
            }
            // table.column
            return Ok(Expr::Column(ColumnRef {
                database: None,
                table: Some(first),
                column: second,
                column_index: None,
            }));
        }

        // Simple column reference
        Ok(Expr::Column(ColumnRef::new(first)))
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr> {
        let distinct = self.match_token(TokenKind::Distinct);

        let args = if self.match_token(TokenKind::Star) {
            FunctionArgs::Star
        } else if self.check(TokenKind::RParen) {
            FunctionArgs::Exprs(Vec::new())
        } else {
            FunctionArgs::Exprs(self.parse_expr_list()?)
        };

        self.expect(TokenKind::RParen)?;

        let filter = if self.match_token(TokenKind::Filter) {
            self.expect(TokenKind::LParen)?;
            self.expect(TokenKind::Where)?;
            let expr = self.parse_expr()?;
            self.expect(TokenKind::RParen)?;
            Some(Box::new(expr))
        } else {
            None
        };

        let over = if self.match_token(TokenKind::Over) {
            if self.match_token(TokenKind::LParen) {
                self.pos -= 1; // Put back the ( for parse_window_spec
                Some(Over::Spec(self.parse_window_spec()?))
            } else {
                Some(Over::Window(self.expect_identifier()?))
            }
        } else {
            None
        };

        Ok(Expr::Function(FunctionCall {
            name,
            args,
            distinct,
            filter,
            over,
        }))
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    fn current(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn current_text(&self) -> &str {
        self.current().text(self.source)
    }

    fn peek(&self) -> &Token {
        if self.pos + 1 < self.tokens.len() {
            &self.tokens[self.pos + 1]
        } else {
            &self.tokens[self.tokens.len() - 1]
        }
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
    }

    fn check(&self, kind: TokenKind) -> bool {
        self.current().kind == kind
    }

    fn check_keyword(&self) -> bool {
        self.current().kind.is_keyword()
    }

    fn check_keyword_for_alias(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::Where
                | TokenKind::Group
                | TokenKind::Having
                | TokenKind::Order
                | TokenKind::Limit
                | TokenKind::Union
                | TokenKind::Intersect
                | TokenKind::Except
                | TokenKind::On
                | TokenKind::Using
                | TokenKind::Join
                | TokenKind::Inner
                | TokenKind::Left
                | TokenKind::Right
                | TokenKind::Full
                | TokenKind::Cross
                | TokenKind::Natural
                | TokenKind::Set
                | TokenKind::From
                | TokenKind::Values
                | TokenKind::Returning
        )
    }

    fn match_token(&mut self, kind: TokenKind) -> bool {
        if self.check(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<()> {
        if self.check(kind) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("expected {:?}", kind)))
        }
    }

    fn expect_identifier(&mut self) -> Result<String> {
        if self.check(TokenKind::Identifier) || self.current().kind.is_keyword() {
            let text = self.current_text();
            // Handle quoted identifiers
            let name = if text.starts_with('"') || text.starts_with('`') || text.starts_with('[') {
                let inner = &text[1..text.len() - 1];
                inner.replace("\"\"", "\"").replace("``", "`")
            } else {
                text.to_string()
            };
            self.advance();
            Ok(name)
        } else {
            Err(self.error("expected identifier"))
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.check(TokenKind::Identifier) && self.current_text().eq_ignore_ascii_case(keyword) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("expected {}", keyword)))
        }
    }

    fn expect_string(&mut self) -> Result<String> {
        if self.check(TokenKind::String) {
            let text = self.current_text();
            let inner = &text[1..text.len() - 1];
            let value = inner.replace("''", "'");
            self.advance();
            Ok(value)
        } else {
            Err(self.error("expected string"))
        }
    }

    fn expect_integer(&mut self) -> Result<i64> {
        if self.check(TokenKind::Integer) {
            let text = self.current_text();
            let value = text
                .parse()
                .map_err(|_| Error::with_message(ErrorCode::Error, "invalid integer"))?;
            self.advance();
            Ok(value)
        } else {
            Err(self.error("expected integer"))
        }
    }

    fn skip_semicolons(&mut self) {
        while self.match_token(TokenKind::Semicolon) {}
    }

    fn parse_qualified_name(&mut self) -> Result<QualifiedName> {
        let first = self.expect_identifier()?;

        if self.match_token(TokenKind::Dot) {
            let second = self.expect_identifier()?;
            Ok(QualifiedName::with_schema(first, second))
        } else {
            Ok(QualifiedName::new(first))
        }
    }

    fn parse_maybe_qualified_name(&mut self) -> Result<(Option<String>, String)> {
        let first = self.expect_identifier()?;

        if self.match_token(TokenKind::Dot) {
            let second = self.expect_identifier()?;
            Ok((Some(first), second))
        } else {
            Ok((None, first))
        }
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut names = vec![self.expect_identifier()?];
        while self.match_token(TokenKind::Comma) {
            names.push(self.expect_identifier()?);
        }
        Ok(names)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![self.parse_expr()?];
        while self.match_token(TokenKind::Comma) {
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    fn error(&self, msg: &str) -> Error {
        let token = self.current();
        Error::with_message(
            ErrorCode::Error,
            format!("{} at line {}, column {}", msg, token.line, token.column),
        )
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Parse a single SQL statement
pub fn parse(sql: &str) -> Result<Stmt> {
    let mut parser = Parser::new(sql)?;
    parser.parse_stmt()
}

/// Parse multiple SQL statements
pub fn parse_all(sql: &str) -> Result<Vec<Stmt>> {
    let mut parser = Parser::new(sql)?;
    let mut stmts = Vec::new();

    while !parser.is_eof() {
        stmts.push(parser.parse_stmt()?);
    }

    Ok(stmts)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let stmt = parse("SELECT * FROM users").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_select_columns() {
        let stmt = parse("SELECT id, name, email FROM users").unwrap();
        if let Stmt::Select(select) = stmt {
            if let SelectBody::Select(core) = select.body {
                assert_eq!(core.columns.len(), 3);
            } else {
                panic!("expected SelectBody::Select");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_parse_select_where() {
        let stmt = parse("SELECT * FROM users WHERE id = 1").unwrap();
        if let Stmt::Select(select) = stmt {
            if let SelectBody::Select(core) = select.body {
                assert!(core.where_clause.is_some());
            } else {
                panic!("expected SelectBody::Select");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_parse_select_join() {
        let stmt = parse("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        assert!(matches!(stmt, Stmt::Insert(_)));
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        assert!(matches!(stmt, Stmt::Update(_)));
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(stmt, Stmt::Delete(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let stmt =
            parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        assert!(matches!(stmt, Stmt::CreateTable(_)));
    }

    #[test]
    fn test_parse_create_index() {
        let stmt = parse("CREATE INDEX idx_users_name ON users (name)").unwrap();
        assert!(matches!(stmt, Stmt::CreateIndex(_)));
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS users").unwrap();
        assert!(matches!(stmt, Stmt::DropTable(_)));
    }

    #[test]
    fn test_parse_begin_commit() {
        let stmt = parse("BEGIN TRANSACTION").unwrap();
        assert!(matches!(stmt, Stmt::Begin(_)));

        let stmt = parse("COMMIT").unwrap();
        assert!(matches!(stmt, Stmt::Commit));
    }

    #[test]
    fn test_parse_expr_arithmetic() {
        let stmt = parse("SELECT 1 + 2 * 3").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_expr_comparison() {
        let stmt = parse("SELECT * FROM t WHERE a = 1 AND b > 2 OR c < 3").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_expr_between() {
        let stmt = parse("SELECT * FROM t WHERE x BETWEEN 1 AND 10").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_expr_in() {
        let stmt = parse("SELECT * FROM t WHERE x IN (1, 2, 3)").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_expr_like() {
        let stmt = parse("SELECT * FROM t WHERE name LIKE '%foo%'").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_expr_case() {
        let stmt = parse("SELECT CASE WHEN x = 1 THEN 'one' ELSE 'other' END FROM t").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_function_call() {
        let stmt = parse("SELECT COUNT(*), MAX(id), SUM(amount) FROM orders").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_subquery() {
        let stmt = parse("SELECT * FROM (SELECT id FROM users) AS sub").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_union() {
        let stmt = parse("SELECT id FROM users UNION SELECT id FROM admins").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_cte() {
        let stmt = parse("WITH cte AS (SELECT 1) SELECT * FROM cte").unwrap();
        assert!(matches!(stmt, Stmt::Select(_)));
    }

    #[test]
    fn test_parse_order_by() {
        let stmt = parse("SELECT * FROM users ORDER BY name ASC, id DESC").unwrap();
        if let Stmt::Select(select) = stmt {
            assert!(select.order_by.is_some());
            let order_by = select.order_by.unwrap();
            assert_eq!(order_by.len(), 2);
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_parse_limit() {
        let stmt = parse("SELECT * FROM users LIMIT 10 OFFSET 5").unwrap();
        if let Stmt::Select(select) = stmt {
            assert!(select.limit.is_some());
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_parse_multiple_statements() {
        let stmts = parse_all("SELECT 1; SELECT 2; SELECT 3").unwrap();
        assert_eq!(stmts.len(), 3);
    }
}

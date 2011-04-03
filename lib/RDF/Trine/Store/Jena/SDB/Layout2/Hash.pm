package RDF::Trine::Store::Jena::SDB::Layout2::Hash;

use 5.008;
use base qw[RDF::Trine::Store];
use common::sense;

use DBI;
use Digest;
use Math::BigInt;
use RDF::Trine;
use RDF::Trine::Error;
use Scalar::Util qw[blessed];

our $VERSION = '0.001';
our $IGNORE_CLEANUP;

sub _new_with_string
{
	my ($class, $config) = @_;
	my ($dsn, $user, $pass) = split /;/, $config;
	my $dbh = DBI->connect($dsn, $user, $pass)
		or die "Could not connect to database: $dsn\n";
	return $class->_new_with_object($dbh);
}

sub _new_with_config
{
	my ($class, $config) = @_;
	my $dbh = DBI->connect($config->{dsn}, $config->{username}, $config->{password}, $config->{attributes})
		or die "Could not connect to database: ".$config->{dsn}."\n";
	return $class->_new_with_object($dbh);
}

sub _new_with_object
{
	my ($class, $database) = @_;
	
#	warn "Table structure looks wrong - perhaps not an SDB Database?"
#		unless (
#			$database->tables(undef, undef, 'quads', 'TABLE') and
#			$database->tables(undef, undef, 'triples', 'TABLE') and
#			$database->tables(undef, undef, 'nodes', 'TABLE')
#			);
	
	my $self = bless {
		dbh => $database,
		}, $class;
	return $self;
}

*new_with_config = \&_new_with_config;
*new_with_object = \&_new_with_object;
*new_with_string = \&_new_with_string;

sub temporary_store
{
	my ($class) = @_;
	my $self = $class->_new_with_config({ dsn => 'dbi:SQLite:dbname=:memory:'});
	$self->init;
	return $self;
}

sub dbh
{
	my ($self) = @_;
	return $self->{dbh};
}

sub init
{
	my ($self) = @_;
	my $dbh = $self->dbh;
	
	unless ($dbh->tables(undef, undef, 'prefixes', 'TABLE'))
	{
		$dbh->do(<<END) || return undef;
			CREATE TABLE prefixes (
				prefix character varying(50) NOT NULL,
				uri character varying(500) NOT NULL,
				PRIMARY KEY (prefix)
				);
END
	}
	
	unless ($dbh->tables(undef, undef, 'nodes', 'TABLE'))
	{
		$dbh->do(<<END) || return undef;
			CREATE TABLE nodes (
				hash bigint NOT NULL,
				lex text NOT NULL,
				lang varchar NOT NULL DEFAULT '',
				datatype varchar(200) NOT NULL DEFAULT '',
				type integer NOT NULL DEFAULT 0,
				PRIMARY KEY (hash)
				);
END
	}

	unless ($dbh->tables(undef, undef, 'quads', 'TABLE'))
	{
		$dbh->do(<<END) || return undef;
			CREATE TABLE quads (
				g bigint NOT NULL,
				s bigint NOT NULL,
				p bigint NOT NULL,
				o bigint NOT NULL,
				PRIMARY KEY (g, s, p, o)
				);
END
		my %indices = (
			graobjsubj   => 'g, o, s',
			grapredobj   => 'g, p, o',
			grasubjpred  => 'g, s, p',
			objsubjgra   => 'o, s, g',
			predobjgra   => 'p, o, g',
		);
		while (my ($name, $cols) = each %indices)
		{
			$dbh->do(sprintf("CREATE INDEX %s ON quads (%s);", $name, $cols));
		}
	}

	unless ($dbh->tables(undef, undef, 'triples', 'TABLE'))
	{
		$dbh->do(<<END) || return undef;
			CREATE TABLE triples (
				s bigint NOT NULL,
				p bigint NOT NULL,
				o bigint NOT NULL,
				PRIMARY KEY (s, p, o)
				);
END
		my %indices = (
			objsubj   => 'o, s',
			predobj   => 'p, o',
		);
		while (my ($name, $cols) = each %indices)
		{
			$dbh->do(sprintf("CREATE INDEX %s ON triples (%s);", $name, $cols));
		}
	}
	
	return $self;
}

sub clear_restrictions
{
	die "Need to enforce RDF data model restrictions for Jena-compatibility.\n";
}

sub SDB_vType
{
	my ($node) = @_;
	
	return 1 if $node->is_blank;
	return 2 if $node->is_resource;
	return 3 if $node->is_literal && !$node->has_datatype;
	return 8 if $node->is_variable;
	
	my $weird = {
		'http://www.w3.org/2001/XMLSchema#string'   => 4,
		'http://www.w3.org/2001/XMLSchema#integer'  => 5,
		'http://www.w3.org/2001/XMLSchema#double'   => 6,
		'http://www.w3.org/2001/XMLSchema#dateTime' => 7,
		}->{ $node->literal_datatype };
		
	return $weird if defined $weird;
	
	return 50;
}

sub SDB_lex
{
	my ($node) = @_;
	return $node->blank_identifier if $node->is_blank;
	return $node->uri if $node->is_resource;
	return $node->literal_value if $node->is_literal;
	return $node->name if $node->is_variable;
}

sub SDB_hash
{
	my ($node) = @_;
	
	my $toHash;
	if ($node->is_literal)
	{
		$toHash = sprintf('%s|%s|%s',
			$node->literal_value,
			$node->literal_value_language||"",
			$node->literal_datatype||"",
			);
	}
	else
	{
		$toHash = sprintf('%s||', SDB_lex($node));
	}
	utf8::encode($toHash);
	
	my $md5 = Digest->new('MD5');
	$md5->add($toHash);
	$md5->add(chr(SDB_vType($node)));
	
	# Only want least significant 64 bits of hash.
	my $ls64_hex    = substr($md5->hexdigest, 16, 16);
	my $ls64_bignum = Math::BigInt->new('0x' . $ls64_hex);
	
	return 
		Math::BigInt->new('0x10000000000000000')->bneg + $ls64_bignum
		if $ls64_hex =~ /^[89ABCDEF]/i;
	
	return $ls64_bignum;
}

sub get_statements
{
	my $self        = shift;
	my @nodes       = @_[0..3];
	my $use_quad    = (scalar(@_) >= 4) ? 1 : 0;
	
	my $sql = $self->_sql({}, @_);
	
	my $sth = $self->dbh->prepare($sql);
	$sth->execute;
	
	my %nodes;
	@nodes{qw[s p o g]} = map
		{ blessed($_) && $_->is_variable ? undef : $_ } 
		@nodes;
	
	my $sub = sub
	{
		my $row  = $sth->fetchrow_hashref;
		return undef unless defined $row;
		my @bits = $use_quad ? qw[s p o g] : qw[s p o];
		my %result = %nodes;
		foreach my $bit (@bits)
		{
			$result{$bit} ||= $self->_extract_node($bit, $row);
			$result{$bit} ||= RDF::Trine::Node::Nil->new if $bit eq 'g';  # fallback
		}
		return $use_quad
			? RDF::Trine::Statement::Quad->new( @result{@bits} )
			: RDF::Trine::Statement->new( @result{@bits} );
	};
	
	return RDF::Trine::Iterator::Graph->new($sub);
}

sub count_statements
{
	my $self = shift;
	
	my $sth  = $self->dbh->prepare($self->_sql({count=>1}, @_));
	$sth->execute;
	my $row  = $sth->fetchrow_arrayref;
	return $row->[0];
}

sub size
{
	my ($self) = @_;
	eval {
		my $sql = <<SQL;
SELECT SUM(c) AS cx
FROM (
	SELECT COUNT(*) AS c FROM quads
	UNION
	SELECT COUNT(*) AS c FROM triples
	) AS st;
SQL
		my $sth = $self->dbh->prepare($sql);
		$sth->execute;
		return $sth->fetchrow_arrayref->[0];
	};
	
	# slower method (for SQLite)
	my $count = 0;
	foreach my $table (qw[quads triples])
	{
		my $sql = sprintf('SELECT COUNT(*) AS cx FROM %s', $table);
		my $sth = $self->dbh->prepare($sql);
		$sth->execute;
		$count += $sth->fetchrow_arrayref->[0];
	}
	return $count;
}

sub get_contexts
{
	my ($self) = @_;
	my $sql = <<SQL;
SELECT DISTINCT n.lex AS g_value, n.lang AS g_lang, n.datatype AS g_datatype, n.type AS g_type
FROM quads AS q
INNER JOIN nodes AS n ON q.g=n.hash;
SQL
	my $sth = $self->dbh->prepare($sql);
	$sth->execute;

	my $sub = sub
	{
		my $row  = $sth->fetchrow_hashref;
		return undef unless defined $row;
		return $self->_extract_node('g', $row);
	};
	
	return RDF::Trine::Iterator->new($sub);
}

sub add_statement
{
	my ($self, $stmt, $context) = @_;
	
	throw RDF::Trine::Error::MethodInvocationError -text => "no statement passed to add_statement"
		unless blessed($stmt);
	
	throw RDF::Trine::Error::MethodInvocationError -text => "add_statement cannot be called with both a quad and a context"
		if $stmt->isa('RDF::Trine::Statement::Quad') && blessed($context);

	throw RDF::Trine::Error::MethodInvocationError -text => "add_statement cannot be passed a non-RDF statement"
		unless $stmt->rdf_compatible;
	
	my @nodes = $stmt->nodes;
	push @nodes, $context if blessed($context);
	
	my ($sql, $ssql);
	if (blessed($nodes[3]))
	{
		$ssql = 'SELECT 1 FROM quads WHERE s=? AND p=? AND o=? AND g=?';
		$sql  = 'INSERT INTO quads (s, p, o, g) VALUES (?, ?, ?, ?)';
	}
	else
	{
		$ssql = 'SELECT 1 FROM triples WHERE s=? AND p=? AND o=?';
		$sql  = 'INSERT INTO triples (s, p, o) VALUES (?, ?, ?)';
	}
	my @values = map { $self->_add_node($_) } @nodes;
	
	my $sth = $self->dbh->prepare($ssql);
	$sth->execute(@values);
	unless ($sth->fetch)
	{
		my $sth = $self->dbh->prepare($sql);
		return $sth->execute(@values);
	}
	
	return undef;
}

sub _add_node
{
	my ($self, $node) = @_;
	
	my ($sql, $ssql);
	my @values = (SDB_hash($node), SDB_lex($node), SDB_vType($node));
		
	if ($node->is_literal and $node->has_datatype)
	{
		$sql    = 'INSERT INTO nodes (hash, lex, type, datatype) VALUES (?, ?, ?, ?)';
		$ssql   = 'SELECT 1 FROM nodes WHERE hash=? AND lex=? AND type=? AND datatype=?';
		push @values, $node->literal_datatype;
	}
	elsif ($node->is_literal and $node->has_language)
	{
		$sql    = 'INSERT INTO nodes (hash, lex, type, lang) VALUES (?, ?, ?, ?)';
		$ssql   = 'SELECT 1 FROM nodes WHERE hash=? AND lex=? AND type=? AND lang=?';
		push @values, $node->literal_value_language;
	}
	else
	{
		$sql    = 'INSERT INTO nodes (hash, lex, type) VALUES (?, ?, ?)';
		$ssql   = 'SELECT 1 FROM nodes WHERE hash=? AND lex=? AND type=?';
	}
	
	my $sth = $self->dbh->prepare($ssql);
	$sth->execute(@values);
	unless ($sth->fetch)
	{
		my $sth = $self->dbh->prepare($sql);
		$sth->execute(@values);
	}
	
	return $values[0];
}

sub remove_statement
{
	my ($self, $stmt, $context) = @_;
	
	throw RDF::Trine::Error::MethodInvocationError -text => "no statement passed to remove_statement"
		unless blessed($stmt);
	
	throw RDF::Trine::Error::MethodInvocationError -text => "remove_statement cannot be called with both a quad and a context"
		if $stmt->isa('RDF::Trine::Statement::Quad') && blessed($context);
	
	my @nodes = $stmt->nodes;
	my $sth;
	
	if (defined $nodes[3] and !$nodes[3]->is_nil)
	{
		$sth = $self->dbh->prepare("DELETE FROM quads WHERE s=? AND p=? AND o=? AND g=?");
	}
	else
	{
		$sth = $self->dbh->prepare("DELETE FROM triples WHERE s=? AND p=? AND o=?");
		@nodes = @nodes[0..2]; # drop nil context component if it exists.
	}
	
	$sth->execute(map { SDB_hash($_) } @nodes);
}

sub remove_statements
{
	my %nodes;
	(my $self, @nodes{qw[s p o g]}) = @_;
	
	my (@whereQ, @whereT);
	foreach my $pos (qw[s p o g])
	{
		push @whereQ, "${pos}=?"
			if defined $nodes{$pos};
		push @whereT, "${pos}=?"
			if defined $nodes{$pos} && $pos ne 'g';
	}
	
	my $whereQ = join ' AND ', @whereQ;
	my $sqlQ   = 'DELETE FROM quads' . ($whereQ ? " WHERE $whereQ" : '');	
	my $sthQ = $self->dbh->prepare($sqlQ);	
	$sthQ->execute(map { SDB_hash($_) } @nodes{qw[s p o g]});
	
	unless (blessed($nodes{g}) and !$nodes{g}->is_nil)
	{
		my $whereT = join ' AND ', @whereT;
		my $sqlT   = 'DELETE FROM triples' . ($whereT ? " WHERE $whereT" : '');	
		my $sthT = $self->dbh->prepare($sqlT);
		$sthT->execute(map { SDB_hash($_) } @nodes{qw[s p o]});
	}
}

sub _sql
{
	my $self     = shift;
	my $opts     = shift;
	my @nodes    = @_[0..3];
	my $use_quad = (scalar(@_) >= 4) ? 1 : 0;

	my %nodes;
	@nodes{qw[s p o g]} = map
		{ blessed($_) && $_->is_variable ? undef : $_ } 
		@nodes;
	
	my $triple_condition = 
	my $quad_condition = 
		join ' AND ',
		grep { defined $_; }
		map { defined $nodes{$_} ? sprintf('%s=%s', $_, SDB_hash($nodes{$_})) : undef }
		qw[s p o g];
	
	$triple_condition ||= '1=1';
	$quad_condition   ||= '1=1';
	
	if ($use_quad and blessed($nodes{g}) and $nodes{g}->is_nil)
		{ $quad_condition = '1=0'; }
	elsif ($use_quad and $nodes{g})
		{ $triple_condition = '1=0'; }

	my $Selectors;
	my $Joins;
	my $Distinct = 'DISTINCT';
	
	if ($opts->{count})
	{
		$Selectors = "\tCOUNT(*) AS c";
		$Joins     = '';
		$Distinct  = '';
	}
	else
	{
		my @positions = $use_quad ? qw[s p o g] : qw[s p o];
		foreach my $pos (@positions)
		{
			unless (defined $nodes{$pos})
			{
				$Selectors .= "\tn${pos}.lex AS ${pos}_value, n${pos}.lang AS ${pos}_lang, n${pos}.datatype AS ${pos}_datatype, n${pos}.type AS ${pos}_type,\n";
				$Joins .= ($pos eq 'g' ? 'LEFT' : 'INNER')." JOIN nodes n${pos} ON n${pos}.hash=st.${pos}\n";
			}
		}
		$Selectors .= "\t1 as existance";
	}
	
	my $quad_cols   = 's, p, o';
	my $triple_cols = 's, p, o';
	if ($use_quad)
	{
		$quad_cols   = 's, p, o, g';
		$triple_cols = 's, p, o, 0 as g';
	}
	
	my $sql = <<SQL;
SELECT $Distinct
$Selectors
FROM (
	(SELECT DISTINCT $quad_cols
	FROM quads
	WHERE $quad_condition)
	UNION
	(SELECT DISTINCT $triple_cols
	FROM triples
	WHERE $triple_condition)
	) AS st
$Joins
SQL
	return $sql;
}

sub _extract_node
{
	my ($self, $var, $row) = @_;
	
	if (!defined $row->{"${var}_type"})
	{
		return undef;
	}

	my $value = $row->{"${var}_value"};
	utf8::decode($value);
	
	if ($row->{"${var}_type"} == 1)
	{
		return RDF::Trine::Node::Blank->new($value);
	}

	if ($row->{"${var}_type"} == 2)
	{
		return RDF::Trine::Node::Resource->new($value);
	}

	if ($row->{"${var}_type"} == 8) # should never happen anyway
	{
		return RDF::Trine::Node::Variable->new($value);
	}

	return RDF::Trine::Node::Literal->new(
		$value,
		$row->{"${var}_lang"},
		$row->{"${var}_datatype"},
		);
}

1;

__END__

=head1 NAME

RDF::Trine::Store::Jena::SDB::Layout2::Hash - connect to Jena SDB stores in
layout2 hash format

=head1 DESCRIPTION

This module inherits from L<RDF::Trine::Store> so provides the constructors
and methods described there. It also provides a method C<init> analogous to
that from L<RDF::Trine::Store::DBI> which creates the table structures and
indexes in a blank database. 

This should theoretically work on any reasonably SQL-compliant database
supported by L<DBI>. Tested:

=over

=item * B<PostgreSQL> - works

=item * B<SQLite> - C<get_statements> fails

=back

=head1 SEE ALSO

L<RDF::Trine::Store>.

L<http://www.perlrdf.org/>.

L<http://openjena.org/wiki/SDB/Database_Layouts>.

=head1 AUTHOR

Toby Inkster E<lt>tobyink@cpan.orgE<gt>.

=head1 COPYRIGHT

Copyright 2011 Toby Inkster

This library is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

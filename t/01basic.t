use Test::More tests => 3;
use RDF::Trine qw[iri literal statement];
BEGIN { use_ok('RDF::Trine::Store::Jena::SDB::Layout2::Hash') };

my $store = RDF::Trine::Store::Jena::SDB::Layout2::Hash->temporary_store;
ok($store, "Able to create temporary store.");

$store->add_statement(
	statement(
		iri('http://example.com/joe#me'),
		iri('http://xmlns.com/foaf/0.1/name'),
		literal('Joe Bloggs'),
		iri('http://example.com/joe'),
		)
	);

is($store->size, 1, 'Able to store a statement.');

#$store->get_statements(undef, iri("http://xmlns.com/foaf/0.1/name"), literal("Joe Bloggs"))->each(sub{
#	my $st = shift;
#	is($st->subject->uri, 'http://example.com/joe#me', 'Able to retrieve statement.');
#});

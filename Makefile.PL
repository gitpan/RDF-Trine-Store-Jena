use strict;
use warnings;

use inc::Module::Install;

my $dist = 'RDF-Trine-Store-Jena';
my $fn   = "lib/RDF/Trine/Store/Jena/SDB/Layout2/Hash.pm";

name                $dist;
perl_version_from   $fn;
version_from        $fn;
abstract_from       $fn;
readme_from         $fn;
author              'Toby Inkster <tobyink@cpan.org>';
license             'perl';

requires            'common::sense'      => '3';
requires            'DBI'                => 0;
requires            'Digest'             => 0;
requires            'Math::BigInt'       => 0;
requires            'RDF::Trine'         => '0.133';
requires            'Scalar::Util'       => 0;
test_requires       'Test::More'         => '0.61';

# install_script 'fingerw';

resources(
	'homepage'   => "http://search.cpan.org/dist/$dist/",
	'repository' => "http://goddamn.co.uk/viewvc/perlmods/$dist/",
	'bugtracker' => "http://rt.cpan.org/Dist/Display.html?Queue=$dist",
	);

keywords(qw[Perl Jena SDB Java RDF SemWeb Semantic Web Trine Query SPARQL DBI]);
	
write_doap_changes;
write_doap_changes_xml;

auto_install;
WriteAll(
	'meta' => 1,
	'sign' => 1,
	);

# See bottom of file for license and copyright information
package Foswiki::Store::SearchAlgorithms::Versatile;

=begin TML

---+ package Foswiki::Store::SearchAlgorithms::Forking
Implements Foswiki::Store::Interfaces::SearchAlgorithm

Forking implementation of flat file store search. Uses grep.

=cut

use strict;
use warnings;
use Assert;

use Foswiki::Store::Interfaces::QueryAlgorithm ();
our @ISA = ('Foswiki::Store::Interfaces::QueryAlgorithm');

use Foswiki::Store::Interfaces::SearchAlgorithm ();
use Foswiki::Search::Node                       ();
use Foswiki::Search::InfoCache                  ();
use Foswiki::Search::ResultSet                  ();
use Foswiki();
use Foswiki::Func();
use Foswiki::Meta            ();
use Foswiki::MetaCache       ();
use Foswiki::Query::Node     ();
use Foswiki::Query::HoistREs ();
use Foswiki::ListIterator();
use Foswiki::Iterator::FilterIterator();
use Foswiki::Iterator::ProcessIterator();

use constant MONITOR => 0;

=begin TML

---++ ClassMethod new( $class,  ) -> $cereal

=cut

sub new {
    my $self = shift()->SUPER::new( 'SEARCH', @_ );
    return $self;
}

#ok, for initial validation, naively call the code with a web.
sub _webQuery {
    my ( $this, $query, $web, $inputTopicSet, $session, $options ) = @_;
    ASSERT( !$query->isEmpty() ) if DEBUG;

    #print STDERR "ForkingSEARCH(".join(', ', @{ $query->tokens() }).")\n";
    # default scope is 'text'
    $options->{'scope'} = 'text'
      unless ( defined( $options->{'scope'} )
        && $options->{'scope'} =~ /^(topic|all)$/ );

    my $topicSet = $inputTopicSet;
    if ( !defined($topicSet) ) {

        #then we start with the whole web
        #TODO: i'm sure that is a flawed assumption
        my $webObject = Foswiki::Meta->new( $session, $web );
        $topicSet =
          Foswiki::Search::InfoCache::getTopicListIterator( $webObject,
            $options );
    }
    ASSERT( UNIVERSAL::isa( $topicSet, 'Foswiki::Iterator' ) ) if DEBUG;

    #print STDERR "######## Forking search ($web) tokens "
    # .scalar(@{$query->tokens()})." : ".join(',', @{$query->tokens()})."\n";
    # AND search - search once for each token, ANDing result together
    foreach my $token ( @{ $query->tokens() } ) {

        my $tokenSQL;
        if( $token =~ m/^:sql:(.*)/ ) {
            $token = $tokenSQL = $1;
        }
        elsif( $token =~ m/^:perl:(.*?):sql:(.*)/ ) {
            ($token, $tokenSQL) = ($1, $2);
        }
        else {
            undef $tokenSQL;
        }

        my $tokenCopy = $token;

        # flag for AND NOT search
        my $invertSearch = 0;
        $invertSearch = ( $tokenCopy =~ s/^\!//o );

        # scope can be 'topic' (default), 'text' or "all"
        # scope='topic', e.g. Perl search on topic name:
        my %topicMatches;
        unless ( $options->{'scope'} eq 'text' ) {
            my $qtoken = $tokenCopy;

# FIXME I18N
# http://foswiki.org/Tasks/Item1646 this causes us to use/leak huge amounts of memory if called too often
            $qtoken = quotemeta($qtoken) if ( $options->{'type'} ne 'regex' );

            $topicSet->reset();
            while ( $topicSet->hasNext() ) {
                my $webtopic = $topicSet->next();
                my ( $itrWeb, $topic ) =
                  Foswiki::Func::normalizeWebTopicName( $web, $webtopic );

                if ( $options->{'casesensitive'} ) {

                    # fix for Codev.SearchWithNoPipe
                    #push(@scopeTopicList, $topic) if ( $topic =~ /$qtoken/ );
                    $topicMatches{$topic} = 1 if ( $topic =~ /$qtoken/ );
                }
                else {

                    #push(@scopeTopicList, $topic) if ( $topic =~ /$qtoken/i );
                    $topicMatches{$topic} = 1 if ( $topic =~ /$qtoken/i );
                }
            }
        }

        # scope='text', e.g. grep search on topic text:
        unless ( $options->{'scope'} eq 'topic' ) {
            my $textMatches =
              $session->{store}->_search( $tokenCopy, $web, $topicSet, $session, $options, $tokenSQL );

            #bring the text matches into the topicMatch hash
            if ($textMatches) {
                @topicMatches{ keys %$textMatches } = values %$textMatches;
            }
        }

        my @scopeTextList = ();
        if ($invertSearch) {
            $topicSet->reset();
            while ( $topicSet->hasNext() ) {
                my $webtopic = $topicSet->next();
                my ( $Iweb, $topic ) =
                  Foswiki::Func::normalizeWebTopicName( $web, $webtopic );

                if ( $topicMatches{$topic} ) {
                }
                else {
                    push( @scopeTextList, $topic );
                }
            }
        }
        else {

            #TODO: the sad thing about this is we lose info
            @scopeTextList = keys(%topicMatches);
        }

        # reduced topic list for next token
        $topicSet =
          Foswiki::Search::InfoCache->new( $Foswiki::Plugins::SESSION, $web,
            \@scopeTextList );
    }

    return $topicSet;
}

1;
__END__
Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2008-2011 Foswiki Contributors. Foswiki Contributors
are listed in the AUTHORS file in the root of this distribution.
NOTE: Please extend that file, not this notice.

Additional copyrights apply to some or all of the code in this
file as follows:

Copyright (C) 2002 John Talintyre, john.talintyre@btinternet.com
Copyright (C) 2002-2007 Peter Thoeny, peter@thoeny.org
and TWiki Contributors. All Rights Reserved. TWiki Contributors
are listed in the AUTHORS file in the root of this distribution.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.

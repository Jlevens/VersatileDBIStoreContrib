# See bottom of file for default license and copyright information

package Foswiki::Plugins::VersatileDBIStorePlugin;
use strict;

use Foswiki();
use Foswiki::Store::Versatile ();

require Foswiki::Func;    # The plugins API
require Foswiki::Plugins; # For the API version

our $VERSION = '$Rev$';
our $RELEASE = '$Date: 2012-04-18 18:20:00 +0200 (Tue, 18 Apr 2012) $';
our $SHORTDESCRIPTION = 'Plugin to support the VersatileDBIStoreContrib';

our $NO_PREFS_IN_TOPIC = 1;

sub initPlugin {
    my( $topic, $web, $user, $installWeb ) = @_;

    if( $Foswiki::Plugins::VERSION < 2.0 ) {
        Foswiki::Func::writeWarning( 'Version mismatch between ',
                                     __PACKAGE__, ' and Plugins.pm' );
        return 0;
    }

    Foswiki::Func::registerRESTHandler('connect', \&Foswiki::Store::Versatile::_restCONNECT);       # Test connection to configured database
    Foswiki::Func::registerRESTHandler('dbi', \&Foswiki::Store::Versatile::_restDBI);               # Report DBI stuff
    Foswiki::Func::registerRESTHandler('test', \&Foswiki::Store::Versatile::_restTEST);         # recreate tables and spin thru existing topics and populate DB
    
    Foswiki::Func::registerRESTHandler('wf', \&Foswiki::Store::Versatile::_restWF);                 # view actual Foswiki::Func and Foswiki::Meta returned when reading specific topics (often created for testing)

    return 1;
}

1;
__END__
This copyright information applies to the VersatileDBIStorePlugin

Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2012 Julian Levens

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.

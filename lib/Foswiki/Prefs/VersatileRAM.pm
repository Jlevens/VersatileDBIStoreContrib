# See bottom of file for license and copyright information
package Foswiki::Prefs::VersatileRAM;

use strict;

use Foswiki::Prefs::TopicRAM    ();
use Foswiki::Prefs::BaseBackend ();

our @ISA = ('Foswiki::Prefs::TopicRAM');

# SMELL: I think the copying of ref to/from a Prefs::VersatileRAM object to a topicObject object should be OK, but I'd like to think again

sub new {
    my ( $class, $topicObject ) = @_;

    if(!exists $topicObject->{_PREF_SET}) {        
        print "TopicRAM::new?\n";
        my $this = $class->SUPER::new($topicObject);
        $this = bless( $this, 'Foswiki::Prefs::VersatileRAM' );
        $topicObject->{_PREF_SET} = $this->{values} // {};
        $topicObject->{_PREF_LOCAL} = $this->{local} // {};
        return $this;
    }

    my $this = bless( {}, 'Foswiki::Prefs::VersatileRAM' );

    $this->{values}      = $topicObject->{_PREF_SET} // {};    
    $this->{local}       = $topicObject->{_PREF_LOCAL} // {};
    $this->{topicObject} = $topicObject;

    return $this;
}

1;
__DATA__

Copyright (C) 2010 Foswiki Contributors. Foswiki Contributors
are listed in the AUTHORS file in the root of this distribution.
NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.

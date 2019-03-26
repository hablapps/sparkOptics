package org.hablapps

import org.hablapps.sparkOptics.GlassesFrame.GlassesFrameSyntax
import org.hablapps.sparkOptics.ProtoLens.ProtoLensSyntax

package object sparkOptics {

  object syntax extends ProtoLensSyntax with GlassesFrameSyntax

}

package org.hablapps

import org.hablapps.sparkOptics.ProtoDFTraversal.ProtoDFTraversalSyntax
import org.hablapps.sparkOptics.ProtoLens.ProtoLensSyntax

package object sparkOptics {

  object syntax extends ProtoLensSyntax with ProtoDFTraversalSyntax

}

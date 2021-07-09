/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.patterns.annotations.unstable;

import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class UnstableProcessor extends AbstractProcessor {

  private ProcessingEnvironment env;

  @Override
  public synchronized void init(ProcessingEnvironment pe) {
    this.env = pe;
  }

  @Override
  public SourceVersion getSupportedSourceVersion() { return SourceVersion.latestSupported(); }

  @Override
  public Set<String> getSupportedAnnotationTypes() { return Set.of(Unstable.class.getName()); }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    roundEnv.getElementsAnnotatedWith(Unstable.class)
            .forEach(element -> env.getMessager()
                                   .printMessage(Kind.MANDATORY_WARNING,
                                                 String.format("%s: is marked ad @%s%n", element,
                                                               Unstable.class.getSimpleName()),
                                                 element));
    return true;
  }
}

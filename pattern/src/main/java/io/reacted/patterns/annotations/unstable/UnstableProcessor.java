/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.patterns.annotations.unstable;

import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

@SupportedAnnotationTypes("io.reacted.patterns.annotations.unstable.Unstable")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class UnstableProcessor extends AbstractProcessor {

  private ProcessingEnvironment env;

  @Override
  public synchronized void init(ProcessingEnvironment pe) {
    this.env = pe;
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    if (!roundEnv.processingOver()) {
      annotations.stream()
                 .map(roundEnv::getElementsAnnotatedWith)
                 .flatMap(Set::stream)
                 .map(Element::getEnclosedElements)
                 .flatMap(List::stream)
                 .forEach(element ->  env.getMessager()
                                        .printMessage(Kind.MANDATORY_WARNING,
                                                      String.format("%s : is marked as unstable%n",
                                                                    element), element));
    }
    return true;
  }
}

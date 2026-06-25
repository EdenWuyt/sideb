import { Directive } from '@angular/core';
import { AbstractControl, NG_VALIDATORS, ValidationErrors, Validator } from '@angular/forms';

@Directive({
  selector: '[integerOnly]',
  providers: [
    {
      provide: NG_VALIDATORS,
      useExisting: IntegerOnlyValidator,
      multi: true
    }
  ]
})

export class IntegerOnlyValidator implements Validator {
  validate(control: AbstractControl): ValidationErrors | null {
    const value = control.value;

    if (value === null || value === undefined || value === '') {
      return null;
    }

    return Number.isInteger(Number(value)) ? null : { integerOnly: true };
  }
}
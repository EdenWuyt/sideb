import { AbstractControl, ValidationErrors, ValidatorFn } from '@angular/forms';

export class AccountValidators {
    static passwordUppercase(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const hasUppercase = /[A-Z]/.test(control.value);
            return hasUppercase ? null : { passwordUppercase: true };
        };
    }

    static passwordLowercase(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const hasLowercase = /[a-z]/.test(control.value);
            return hasLowercase ? null : { passwordLowercase: true };
        };
    }

    static passwordNumber(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const hasNumber = /[0-9]/.test(control.value);
            return hasNumber ? null : { passwordNumber: true };
        };
    }

    static passwordSpecialChar(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const hasSpecialChar = /[^A-Za-z0-9]/.test(control.value);
            return hasSpecialChar ? null : { passwordSpecialChar: true };
        };
    }

    static passwordLength(minLength: number = 8): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const isValidLength = control.value && control.value.length >= minLength;
            return isValidLength ? null : { passwordLength: true };
        };
    }

    static matchPassword(passwordControlName: string): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const passwordControl = control.root.get(passwordControlName);
            if (!passwordControl) return null;
            
            return passwordControl.value === control.value
            ? null
            : { matchPassword: true };
        };
    }
}
import { Component, inject, signal } from '@angular/core';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButton } from '@angular/material/button';
import { MatCard } from '@angular/material/card';
import { MatIcon } from '@angular/material/icon';
import { Account } from '../../../core/services/account';
import { Router } from '@angular/router';
import { Snackbar } from '../../../core/services/snackbar';
import { AccountValidators } from '../../../shared/validators/account-validators';
import { TextInput } from '../../../shared/components/text-input/text-input';

@Component({
  selector: 'app-register',
  imports: [
    ReactiveFormsModule,
    MatCard,
    MatButton,
    MatIcon,
    TextInput
  ],
  templateUrl: './register.html',
  styleUrl: './register.css',
})
export class Register {
  private fb = inject(FormBuilder);
  private accountService = inject(Account);
  private router = inject(Router);
  snackbar = inject(Snackbar);
  passwordVisible: boolean = false;
  confirmPasswordVisible: boolean = false;
  validationErrors = signal<string[]>([]);

  registerForm = this.fb.group({
    firstName: ['', Validators.required],
    lastName: ['', Validators.required],
    email: ['', [Validators.required, Validators.email]],
    password: ['', [
      Validators.required, 
      AccountValidators.passwordUppercase(), 
      AccountValidators.passwordLowercase(), 
      AccountValidators.passwordNumber(), 
      AccountValidators.passwordSpecialChar(), 
      AccountValidators.passwordLength(8)
    ]],
    confirmPassword: ['', [Validators.required, AccountValidators.matchPassword('password')]],
  });
  
  onSubmit() {
    if (this.registerForm.valid) {
      this.accountService.register(this.registerForm.value).subscribe({
        next: () => {
          this.snackbar.success("Registration successful! You can now log in.");
          this.router.navigateByUrl('/account/login');
        },
        error: (errors) => {
          console.error(errors);
          this.validationErrors.set(errors);
        }
      });
    }
  }

  togglePasswordVisibility() {
    this.passwordVisible = !this.passwordVisible;
  }

  toggleConfirmPasswordVisibility() {
    this.confirmPasswordVisible = !this.confirmPasswordVisible;
  }
}

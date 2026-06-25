import { Component, inject } from '@angular/core';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButton } from '@angular/material/button';
import { MatCard } from '@angular/material/card';
import { MatIcon } from '@angular/material/icon';
import { Account } from '../../../core/services/account';
import { ActivatedRoute, Router } from '@angular/router';
import { TextInput } from '../../../shared/components/text-input/text-input';

@Component({
  selector: 'app-login',
  imports: [
    ReactiveFormsModule,
    MatCard,
    MatButton,
    MatIcon,
    TextInput,
],
  templateUrl: './login.html',
  styleUrl: './login.css',
})
export class Login {
  private fb = inject(FormBuilder);
  private accountService = inject(Account);
  private router = inject(Router);
  private activatedRoute = inject(ActivatedRoute);
  passwordVisible: boolean = false;
  returnUrl: string = '/shop';

  constructor() {
    const url = this.activatedRoute.snapshot.queryParams['returnUrl'];
    if (url) {
      this.returnUrl = url;
    }
  }

  loginForm = this.fb.group({
    email: ['', [Validators.required, Validators.email]],
    password: ['', Validators.required],
  });

  onSubmit() {
    if (this.loginForm.valid) {
      this.accountService.login(this.loginForm.value).subscribe({
        next: () => {
          this.accountService.getUserInfo().subscribe();  // observable must be subscribed to execute
          this.router.navigateByUrl(this.returnUrl);
        },
        error: (error) => console.error(error)
      });
    }
  }

  togglePasswordVisibility() {
    this.passwordVisible = !this.passwordVisible;
  }
}

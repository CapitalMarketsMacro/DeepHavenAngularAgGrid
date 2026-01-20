import { Component, output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ConnectionConfig } from '../../services/deephaven.service';

@Component({
  selector: 'app-connection-dialog',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './connection-dialog.component.html',
  styleUrl: './connection-dialog.component.scss'
})
export class ConnectionDialogComponent {
  readonly connect = output<ConnectionConfig>();

  serverUrl = signal('http://montunoblenumbat2404:10000');
  authToken = signal('mn2413');
  tableName = signal('');
  isSubmitting = signal(false);
  errorMessage = signal<string | null>(null);

  onSubmit(): void {
    if (!this.serverUrl() || !this.authToken() || !this.tableName()) {
      this.errorMessage.set('Please fill in all fields');
      return;
    }

    this.errorMessage.set(null);
    this.isSubmitting.set(true);

    this.connect.emit({
      serverUrl: this.serverUrl(),
      authToken: this.authToken(),
      tableName: this.tableName()
    });
  }

  setSubmitting(value: boolean): void {
    this.isSubmitting.set(value);
  }

  setError(message: string | null): void {
    this.errorMessage.set(message);
  }
}
